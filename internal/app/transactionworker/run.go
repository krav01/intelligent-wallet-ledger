package transactionworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	quarantinepostgres "github.com/krav01/intelligent-wallet-ledger/internal/quarantine/postgres"
	transferevents "github.com/krav01/intelligent-wallet-ledger/internal/transfer/events"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultTopic    = "wallet.events.v1"
	defaultGroupID  = "intelligent-wallet-ledger-transaction-v1"
	defaultClientID = "intelligent-wallet-ledger-transaction-worker"
	startupTimeout  = 10 * time.Second
)

var errUnsupportedEventType = errors.New("transaction worker: unsupported event type")

// Config contains process-level PostgreSQL and Kafka configuration.
type Config struct {
	DatabaseURL  string
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string
}

// ConfigFromEnv loads transaction-worker configuration from environment variables.
func ConfigFromEnv() Config {
	topic := strings.TrimSpace(os.Getenv("KAFKA_TOPIC"))
	if topic == "" {
		topic = defaultTopic
	}
	groupID := strings.TrimSpace(os.Getenv("KAFKA_TRANSACTION_GROUP_ID"))
	if groupID == "" {
		groupID = defaultGroupID
	}
	return Config{
		DatabaseURL:  strings.TrimSpace(os.Getenv("DATABASE_URL")),
		KafkaBrokers: splitNonempty(os.Getenv("KAFKA_BROKERS")),
		KafkaTopic:   topic,
		KafkaGroupID: groupID,
	}
}

// Run consumes risk-assessed transfer events until the context is cancelled.
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
	handler, err := NewHandler(pool, time.Now)
	if err != nil {
		return fmt.Errorf("creating transaction handler: %w", err)
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

	logger.Info("transaction worker started", "topic", config.KafkaTopic, "group_id", config.KafkaGroupID)
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
					logger.Warn("transaction event quarantined", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "reason_code", reasonCode)
					continue
				}
				return fmt.Errorf("handling Kafka record at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
			}
			if err := client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("committing Kafka record at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
			}
			if !handled {
				logger.Debug("transaction event ignored", "event_id", envelope.EventID(), "event_type", envelope.EventType())
				continue
			}
			logger.Info("transaction event processed", "event_id", envelope.EventID(), "transfer_id", envelope.AggregateID())
		}
	}
}

func handleRecord(
	ctx context.Context,
	handler eventHandler,
	value []byte,
) (event.Envelope, bool, error) {
	envelope, err := event.ParseEnvelope(value)
	if err != nil {
		return event.Envelope{}, false, fmt.Errorf("parsing Kafka event: %w", err)
	}
	if !knownEventType(envelope.EventType()) {
		return envelope, false, fmt.Errorf("%w: %s", errUnsupportedEventType, envelope.EventType())
	}
	if envelope.EventType() != transferevents.RiskAssessedType && envelope.EventType() != transferevents.ReviewDecidedType {
		return envelope, false, nil
	}
	if err := handler.Handle(ctx, envelope); err != nil {
		return event.Envelope{}, false, fmt.Errorf("handling transaction event: %w", err)
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
