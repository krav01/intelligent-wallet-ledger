package riskworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	quarantinepostgres "github.com/krav01/intelligent-wallet-ledger/internal/quarantine/postgres"
	riskdomain "github.com/krav01/intelligent-wallet-ledger/internal/risk/domain"
	transferevents "github.com/krav01/intelligent-wallet-ledger/internal/transfer/events"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultTopic    = "wallet.events.v1"
	defaultGroupID  = "intelligent-wallet-ledger-risk-v1"
	defaultClientID = "intelligent-wallet-ledger-risk-worker"
	startupTimeout  = 10 * time.Second
)

var errUnsupportedEventType = errors.New("risk worker: unsupported event type")

// Config contains process-level PostgreSQL, Kafka, and immutable risk policy configuration.
type Config struct {
	DatabaseURL  string
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string
	Policies     []riskdomain.Policy
	Velocity     VelocityConfig
}

// VelocityConfig contains the Redis connection and sliding-window configuration.
type VelocityConfig struct {
	RedisAddress string
	Window       time.Duration
}

// Enabled reports whether Redis velocity observation is configured.
func (c VelocityConfig) Enabled() bool { return c.RedisAddress != "" }

// ConfigFromEnv loads the risk-worker configuration from environment variables.
func ConfigFromEnv() (Config, error) {
	policies, err := policiesFromEnv()
	if err != nil {
		return Config{}, err
	}
	velocity, err := velocityConfigFromEnv(policies)
	if err != nil {
		return Config{}, err
	}
	topic := strings.TrimSpace(os.Getenv("KAFKA_TOPIC"))
	if topic == "" {
		topic = defaultTopic
	}
	groupID := strings.TrimSpace(os.Getenv("KAFKA_GROUP_ID"))
	if groupID == "" {
		groupID = defaultGroupID
	}

	return Config{
		DatabaseURL:  strings.TrimSpace(os.Getenv("DATABASE_URL")),
		KafkaBrokers: splitNonempty(os.Getenv("KAFKA_BROKERS")),
		KafkaTopic:   topic,
		KafkaGroupID: groupID,
		Policies:     policies,
		Velocity:     velocity,
	}, nil
}

// Run consumes requested transfer events until the context is cancelled.
func Run(ctx context.Context, config Config, velocity VelocityObserver, logger *slog.Logger) error {
	if logger == nil {
		return errors.New("logger is required")
	}
	if config.DatabaseURL == "" {
		return errors.New("database URL is required")
	}
	if len(config.KafkaBrokers) == 0 {
		return errors.New("kafka brokers are required")
	}
	if config.KafkaTopic == "" {
		return errors.New("kafka topic is required")
	}
	if config.KafkaGroupID == "" {
		return errors.New("kafka group id is required")
	}
	if config.Velocity.Enabled() && velocity == nil {
		return errors.New("velocity observer is required")
	}
	if velocity == nil {
		velocity = noopVelocityObserver{}
	}

	pool, err := pgxpool.New(ctx, config.DatabaseURL)
	if err != nil {
		return fmt.Errorf("creating PostgreSQL pool: %w", err)
	}
	defer pool.Close()
	startupCtx, cancelStartup := context.WithTimeout(ctx, startupTimeout)
	defer cancelStartup()
	if err := pool.Ping(startupCtx); err != nil {
		return fmt.Errorf("pinging PostgreSQL: %w", err)
	}

	handler, err := NewHandlerWithPoliciesAndVelocity(pool, config.Policies, velocity, logger, time.Now)
	if err != nil {
		return fmt.Errorf("creating risk handler: %w", err)
	}
	quarantine, err := quarantinepostgres.NewRepository(pool)
	if err != nil {
		return fmt.Errorf("creating consumer quarantine repository: %w", err)
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(config.KafkaBrokers...),
		kgo.ClientID(defaultClientID),
		kgo.ConsumerGroup(config.KafkaGroupID),
		kgo.ConsumeTopics(config.KafkaTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return fmt.Errorf("creating Kafka consumer: %w", err)
	}
	defer client.Close()
	if err := client.Ping(startupCtx); err != nil {
		return fmt.Errorf("pinging Kafka: %w", err)
	}
	cancelStartup()

	logger.Info("risk worker started", "topic", config.KafkaTopic, "group_id", config.KafkaGroupID, "policy_count", len(config.Policies))
	return consume(ctx, client, handler, quarantine, logger)
}

type eventHandler interface {
	Handle(context.Context, event.Envelope) error
}

type quarantineStore interface {
	Store(context.Context, quarantinepostgres.Record) error
}

func consume(ctx context.Context, client *kgo.Client, handler eventHandler, quarantine quarantineStore, logger *slog.Logger) error {
	for {
		fetches := client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil
		}
		if err := fetches.Err(); err != nil {
			return fmt.Errorf("polling Kafka: %w", err)
		}
		for _, record := range fetches.Records() {
			envelope, handled, err := handleRecord(ctx, handler, record.Value)
			if err != nil {
				if reasonCode, quarantinable := quarantineReason(err); quarantinable {
					if err := quarantine.Store(ctx, quarantinepostgres.Record{
						ConsumerName: consumerName,
						Topic:        record.Topic,
						Partition:    record.Partition,
						Offset:       record.Offset,
						ReasonCode:   reasonCode,
						Value:        record.Value,
					}); err != nil {
						return fmt.Errorf("quarantining Kafka record at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
					}
					if err := client.CommitRecords(ctx, record); err != nil {
						return fmt.Errorf("committing quarantined Kafka record at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
					}
					logger.Warn("risk event quarantined", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "reason_code", reasonCode)
					continue
				}
				return fmt.Errorf("handling Kafka record at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
			}
			if err := client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("committing Kafka record at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
			}
			if !handled {
				logger.Debug("risk event ignored", "event_id", envelope.EventID(), "event_type", envelope.EventType())
				continue
			}
			logger.Info("risk event processed", "event_id", envelope.EventID(), "transfer_id", envelope.AggregateID())
		}
	}
}

func handleRecord(ctx context.Context, handler eventHandler, value []byte) (event.Envelope, bool, error) {
	envelope, err := event.ParseEnvelope(value)
	if err != nil {
		return event.Envelope{}, false, fmt.Errorf("parsing Kafka event: %w", err)
	}
	if !knownEventType(envelope.EventType()) {
		return envelope, false, fmt.Errorf("%w: %s", errUnsupportedEventType, envelope.EventType())
	}
	if envelope.EventType() != transferevents.RequestedType {
		return envelope, false, nil
	}
	if err := handler.Handle(ctx, envelope); err != nil {
		return event.Envelope{}, false, fmt.Errorf("handling risk event: %w", err)
	}
	return envelope, true, nil
}

func quarantineReason(err error) (string, bool) {
	if errors.Is(err, event.ErrInvalidEnvelope) {
		return quarantinepostgres.ReasonInvalidEnvelope, true
	}
	if errors.Is(err, errUnsupportedEventType) {
		return quarantinepostgres.ReasonUnsupportedEventType, true
	}
	return "", false
}

func knownEventType(eventType string) bool {
	switch eventType {
	case transferevents.RequestedType,
		transferevents.RiskAssessedType,
		transferevents.ReviewDecidedType,
		transferevents.CompletedType,
		transferevents.FailedType:
		return true
	default:
		return false
	}
}

func policiesFromEnv() ([]riskdomain.Policy, error) {
	if encoded := strings.TrimSpace(os.Getenv("RISK_POLICIES_JSON")); encoded != "" {
		return policiesFromJSON(encoded)
	}
	policy, err := policyFromEnv()
	if err != nil {
		return nil, err
	}
	return []riskdomain.Policy{policy}, nil
}

func policyFromEnv() (riskdomain.Policy, error) {
	version := strings.TrimSpace(os.Getenv("RISK_POLICY_VERSION"))
	reviewAmount, err := parsePositiveInt64("RISK_REVIEW_AMOUNT_USD_MINOR", os.Getenv("RISK_REVIEW_AMOUNT_USD_MINOR"))
	if err != nil {
		return riskdomain.Policy{}, err
	}
	declineAmount, err := parsePositiveInt64("RISK_DECLINE_AMOUNT_USD_MINOR", os.Getenv("RISK_DECLINE_AMOUNT_USD_MINOR"))
	if err != nil {
		return riskdomain.Policy{}, err
	}
	velocityReviewCount, err := parseNonNegativeInt("RISK_VELOCITY_REVIEW_TRANSFER_COUNT", os.Getenv("RISK_VELOCITY_REVIEW_TRANSFER_COUNT"))
	if err != nil {
		return riskdomain.Policy{}, err
	}
	currency, err := ledgerdomain.ParseCurrency("USD")
	if err != nil {
		return riskdomain.Policy{}, fmt.Errorf("parsing USD currency: %w", err)
	}
	policy, err := riskdomain.NewPolicy(riskdomain.PolicyParams{
		Version: version,
		Thresholds: []riskdomain.Threshold{{
			Currency:                    currency,
			ReviewAmountMinor:           reviewAmount,
			DeclineAmountMinor:          declineAmount,
			VelocityReviewTransferCount: velocityReviewCount,
		}},
	})
	if err != nil {
		return riskdomain.Policy{}, fmt.Errorf("creating risk policy: %w", err)
	}
	return policy, nil
}

func policiesFromJSON(encoded string) ([]riskdomain.Policy, error) {
	var params []struct {
		Version                     string `json:"version"`
		ReviewUSD                   int64  `json:"review_amount_usd_minor"`
		DeclineUSD                  int64  `json:"decline_amount_usd_minor"`
		VelocityReviewTransferCount int    `json:"velocity_review_transfer_count"`
	}
	if err := json.Unmarshal([]byte(encoded), &params); err != nil || len(params) == 0 {
		return nil, errors.New("parsing RISK_POLICIES_JSON: nonempty policy array required")
	}
	currency, err := ledgerdomain.ParseCurrency("USD")
	if err != nil {
		return nil, fmt.Errorf("parsing USD currency: %w", err)
	}
	policies := make([]riskdomain.Policy, 0, len(params))
	for _, param := range params {
		policy, err := riskdomain.NewPolicy(riskdomain.PolicyParams{Version: param.Version, Thresholds: []riskdomain.Threshold{{Currency: currency, ReviewAmountMinor: param.ReviewUSD, DeclineAmountMinor: param.DeclineUSD, VelocityReviewTransferCount: param.VelocityReviewTransferCount}}})
		if err != nil {
			return nil, fmt.Errorf("creating risk policy: %w", err)
		}
		policies = append(policies, policy)
	}
	return policies, nil
}

func parsePositiveInt64(name, value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, fmt.Errorf("%s is required", name)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("parsing %s: positive integer required", name)
	}
	return parsed, nil
}

func velocityConfigFromEnv(policies []riskdomain.Policy) (VelocityConfig, error) {
	for _, policy := range policies {
		if !policy.RequiresVelocity() {
			continue
		}
		address := strings.TrimSpace(os.Getenv("REDIS_ADDRESS"))
		if address == "" {
			return VelocityConfig{}, errors.New("REDIS_ADDRESS is required when velocity is enabled")
		}
		window, err := time.ParseDuration(strings.TrimSpace(os.Getenv("RISK_VELOCITY_WINDOW")))
		if err != nil || window < time.Millisecond {
			return VelocityConfig{}, errors.New("RISK_VELOCITY_WINDOW must be at least one millisecond when velocity is enabled")
		}
		return VelocityConfig{RedisAddress: address, Window: window}, nil
	}
	return VelocityConfig{}, nil
}

func parseNonNegativeInt(name, value string) (int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 0)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("parsing %s: non-negative integer required", name)
	}
	return int(parsed), nil
}

func splitNonempty(value string) []string {
	var result []string
	for part := range strings.SplitSeq(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
