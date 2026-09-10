//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/outbox"
	outboxkafka "github.com/krav01/intelligent-wallet-ledger/internal/outbox/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestProducerPublishesKeyedRecord(t *testing.T) {
	brokers := kafkaBrokers(t)
	topic := fmt.Sprintf("wallet.events.integration.%d", time.Now().UnixNano())
	producer, err := outboxkafka.NewProducer(outboxkafka.ProducerConfig{
		Brokers:                brokers,
		Topic:                  topic,
		ClientID:               "outbox-producer-integration",
		DeliveryTimeout:        10 * time.Second,
		MaxBufferedRecords:     10,
		AllowAutoTopicCreation: true,
	})
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	defer producer.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := producer.Ping(ctx); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	want := outbox.Message{Key: "transfer:111", Value: []byte(`{"event_id":"111"}`)}
	if err := producer.Publish(ctx, want); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("creating Kafka consumer: %v", err)
	}
	defer consumer.Close()
	for {
		fetches := consumer.PollFetches(ctx)
		if fetchErrors := fetches.Errors(); len(fetchErrors) > 0 {
			t.Fatalf("polling Kafka: %v", fetchErrors)
		}
		for _, record := range fetches.Records() {
			if string(record.Key) != want.Key || string(record.Value) != string(want.Value) {
				t.Fatalf("Kafka record = (%q, %s), want (%q, %s)", record.Key, record.Value, want.Key, want.Value)
			}
			return
		}
		if err := ctx.Err(); err != nil {
			t.Fatalf("waiting for Kafka record: %v", err)
		}
	}
}

func kafkaBrokers(t testing.TB) []string {
	t.Helper()
	value := os.Getenv("KAFKA_BROKERS")
	if value == "" {
		t.Skip("KAFKA_BROKERS is required for Kafka integration tests")
	}
	var brokers []string
	for broker := range strings.SplitSeq(value, ",") {
		broker = strings.TrimSpace(broker)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}
	if len(brokers) == 0 {
		t.Fatal("KAFKA_BROKERS contains no broker addresses")
	}
	return brokers
}
