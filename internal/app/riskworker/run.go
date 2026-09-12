package riskworker

import (
	"context"
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
	riskdomain "github.com/krav01/intelligent-wallet-ledger/internal/risk/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultTopic    = "wallet.events.v1"
	defaultGroupID  = "intelligent-wallet-ledger-risk-v1"
	defaultClientID = "intelligent-wallet-ledger-risk-worker"
	startupTimeout  = 10 * time.Second
)

// Config contains process-level PostgreSQL, Kafka, and immutable risk policy configuration.
type Config struct {
	DatabaseURL  string
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string
	Policy       riskdomain.Policy
}

// ConfigFromEnv loads the risk-worker configuration from environment variables.
func ConfigFromEnv() (Config, error) {
	policy, err := policyFromEnv()
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
		Policy:       policy,
	}, nil
}

// Run consumes requested transfer events until the context is cancelled.
func Run(ctx context.Context, config Config, logger *slog.Logger) error {
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

	handler, err := NewHandler(pool, config.Policy, time.Now)
	if err != nil {
		return fmt.Errorf("creating risk handler: %w", err)
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

	logger.Info("risk worker started", "topic", config.KafkaTopic, "group_id", config.KafkaGroupID, "policy_version", config.Policy.Version())
	return consume(ctx, client, handler, logger)
}

func consume(ctx context.Context, client *kgo.Client, handler *Handler, logger *slog.Logger) error {
	for {
		fetches := client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil
		}
		if err := fetches.Err(); err != nil {
			return fmt.Errorf("polling Kafka: %w", err)
		}
		for _, record := range fetches.Records() {
			envelope, err := handleRecord(ctx, handler, record.Value)
			if err != nil {
				return fmt.Errorf("handling Kafka record at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
			}
			if err := client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("committing Kafka record at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
			}
			logger.Info("risk event processed", "event_id", envelope.EventID(), "transfer_id", envelope.AggregateID())
		}
	}
}

func handleRecord(ctx context.Context, handler interface {
	Handle(context.Context, event.Envelope) error
}, value []byte,
) (event.Envelope, error) {
	envelope, err := event.ParseEnvelope(value)
	if err != nil {
		return event.Envelope{}, fmt.Errorf("parsing Kafka event: %w", err)
	}
	if err := handler.Handle(ctx, envelope); err != nil {
		return event.Envelope{}, fmt.Errorf("handling risk event: %w", err)
	}
	return envelope, nil
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
	currency, err := ledgerdomain.ParseCurrency("USD")
	if err != nil {
		return riskdomain.Policy{}, fmt.Errorf("parsing USD currency: %w", err)
	}
	policy, err := riskdomain.NewPolicy(riskdomain.PolicyParams{
		Version: version,
		Thresholds: []riskdomain.Threshold{{
			Currency:           currency,
			ReviewAmountMinor:  reviewAmount,
			DeclineAmountMinor: declineAmount,
		}},
	})
	if err != nil {
		return riskdomain.Policy{}, fmt.Errorf("creating risk policy: %w", err)
	}
	return policy, nil
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
