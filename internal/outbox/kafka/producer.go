// Package kafka publishes outbox messages through Apache Kafka.
package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/outbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	maxTopicBytes      = 249
	maxClientIDBytes   = 255
	maxBufferedRecords = 10000
	maxDeliveryTimeout = 15 * time.Minute
)

// ProducerConfig configures a bounded, idempotent Kafka producer.
type ProducerConfig struct {
	Brokers                []string
	Topic                  string
	ClientID               string
	DeliveryTimeout        time.Duration
	MaxBufferedRecords     int
	AllowAutoTopicCreation bool
}

// Producer sends messages synchronously with all in-sync replica acknowledgements.
type Producer struct {
	client *kgo.Client
	topic  string
}

var _ outbox.Producer = (*Producer)(nil)

// NewProducer validates configuration and creates a Kafka producer.
func NewProducer(config ProducerConfig) (*Producer, error) {
	brokers, err := validateProducerConfig(config)
	if err != nil {
		return nil, err
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(config.ClientID),
		kgo.DefaultProduceTopic(config.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.RecordDeliveryTimeout(config.DeliveryTimeout),
		kgo.MaxBufferedRecords(config.MaxBufferedRecords),
		kgo.StopProducerOnDataLossDetected(),
	}
	if config.AllowAutoTopicCreation {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating Kafka producer: %w", err)
	}

	return &Producer{client: client, topic: config.Topic}, nil
}

// Ping verifies that at least one configured broker is reachable.
func (p *Producer) Ping(ctx context.Context) error {
	if err := p.client.Ping(ctx); err != nil {
		return fmt.Errorf("pinging Kafka: %w", err)
	}
	return nil
}

// Publish sends one keyed message and waits for its broker acknowledgement.
func (p *Producer) Publish(ctx context.Context, message outbox.Message) error {
	if message.Key == "" {
		return errors.New("kafka message key is required")
	}
	if len(message.Value) == 0 {
		return errors.New("kafka message value is required")
	}
	record := &kgo.Record{
		Topic: p.topic,
		Key:   bytes.Clone([]byte(message.Key)),
		Value: bytes.Clone(message.Value),
	}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("producing Kafka record: %w", err)
	}
	return nil
}

// Close releases producer resources after all synchronous calls return.
func (p *Producer) Close() {
	p.client.Close()
}

func validateProducerConfig(config ProducerConfig) ([]string, error) {
	if len(config.Brokers) == 0 {
		return nil, errors.New("kafka brokers are required")
	}
	brokers := make([]string, len(config.Brokers))
	for index, broker := range config.Brokers {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			return nil, errors.New("kafka broker address is required")
		}
		brokers[index] = broker
	}
	if !validKafkaName(config.Topic, maxTopicBytes) || config.Topic == "." || config.Topic == ".." {
		return nil, errors.New("kafka topic is invalid")
	}
	if config.ClientID == "" || len(config.ClientID) > maxClientIDBytes {
		return nil, errors.New("kafka client ID is invalid")
	}
	if config.DeliveryTimeout <= 0 || config.DeliveryTimeout > maxDeliveryTimeout {
		return nil, fmt.Errorf("kafka delivery timeout must be between 1ns and %s", maxDeliveryTimeout)
	}
	if config.MaxBufferedRecords <= 0 || config.MaxBufferedRecords > maxBufferedRecords {
		return nil, fmt.Errorf("kafka maximum buffered records must be between 1 and %d", maxBufferedRecords)
	}

	return brokers, nil
}

func validKafkaName(value string, maxBytes int) bool {
	if value == "" || len(value) > maxBytes {
		return false
	}
	for _, character := range []byte(value) {
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}
