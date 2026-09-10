// Package outboxpublisher owns the outbox publisher process lifecycle.
package outboxpublisher

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
	"github.com/krav01/intelligent-wallet-ledger/internal/outbox"
	outboxkafka "github.com/krav01/intelligent-wallet-ledger/internal/outbox/kafka"
	outboxpostgres "github.com/krav01/intelligent-wallet-ledger/internal/outbox/postgres"
)

const (
	defaultTopic           = "wallet.events.v1"
	defaultClientID        = "intelligent-wallet-ledger-outbox"
	startupTimeout         = 10 * time.Second
	defaultDeliveryTimeout = 10 * time.Second
	defaultClaimLease      = 30 * time.Second
	defaultPollInterval    = 250 * time.Millisecond
	defaultRetryDelay      = time.Second
	defaultMaxRetryDelay   = time.Minute
	defaultBatchSize       = 100
)

// Config contains process-level PostgreSQL and Kafka configuration.
type Config struct {
	DatabaseURL            string
	KafkaBrokers           []string
	KafkaTopic             string
	AllowAutoTopicCreation bool
}

// ConfigFromEnv loads 12-factor process configuration from environment variables.
func ConfigFromEnv() (Config, error) {
	allowAutoTopicCreation, err := parseOptionalBool(
		"KAFKA_ALLOW_AUTO_TOPIC_CREATION",
		os.Getenv("KAFKA_ALLOW_AUTO_TOPIC_CREATION"),
	)
	if err != nil {
		return Config{}, err
	}
	topic := strings.TrimSpace(os.Getenv("KAFKA_TOPIC"))
	if topic == "" {
		topic = defaultTopic
	}

	return Config{
		DatabaseURL:            strings.TrimSpace(os.Getenv("DATABASE_URL")),
		KafkaBrokers:           splitNonempty(os.Getenv("KAFKA_BROKERS")),
		KafkaTopic:             topic,
		AllowAutoTopicCreation: allowAutoTopicCreation,
	}, nil
}

// Run publishes durable outbox events until the context is cancelled.
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

	store, err := outboxpostgres.NewRepository(pool)
	if err != nil {
		return fmt.Errorf("creating outbox repository: %w", err)
	}
	producer, err := outboxkafka.NewProducer(outboxkafka.ProducerConfig{
		Brokers:                config.KafkaBrokers,
		Topic:                  config.KafkaTopic,
		ClientID:               defaultClientID,
		DeliveryTimeout:        defaultDeliveryTimeout,
		MaxBufferedRecords:     defaultBatchSize,
		AllowAutoTopicCreation: config.AllowAutoTopicCreation,
	})
	if err != nil {
		return fmt.Errorf("creating Kafka adapter: %w", err)
	}
	defer producer.Close()
	if err := producer.Ping(startupCtx); err != nil {
		return err
	}
	cancelStartup()

	publisher, err := outbox.NewPublisher(store, producer, logger, outbox.PublisherConfig{
		BatchSize:      defaultBatchSize,
		ClaimLease:     defaultClaimLease,
		PollInterval:   defaultPollInterval,
		BaseRetryDelay: defaultRetryDelay,
		MaxRetryDelay:  defaultMaxRetryDelay,
	})
	if err != nil {
		return fmt.Errorf("creating outbox publisher: %w", err)
	}
	logger.Info(
		"outbox publisher started",
		"topic", config.KafkaTopic,
		"batch_size", defaultBatchSize,
		"claim_lease", defaultClaimLease,
	)

	return publisher.Run(ctx)
}

func parseOptionalBool(name, value string) (bool, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("parsing %s: %w", name, err)
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
