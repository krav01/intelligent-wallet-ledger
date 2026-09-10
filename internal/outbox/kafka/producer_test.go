package kafka

import (
	"testing"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/outbox"
)

func TestNewProducerRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()
	valid := validProducerConfig()
	tests := []struct {
		name   string
		change func(*ProducerConfig)
	}{
		{name: "missing brokers", change: func(config *ProducerConfig) { config.Brokers = nil }},
		{name: "empty broker", change: func(config *ProducerConfig) { config.Brokers = []string{" "} }},
		{name: "invalid topic", change: func(config *ProducerConfig) { config.Topic = "wallet events" }},
		{name: "missing client ID", change: func(config *ProducerConfig) { config.ClientID = "" }},
		{name: "missing timeout", change: func(config *ProducerConfig) { config.DeliveryTimeout = 0 }},
		{name: "missing buffer limit", change: func(config *ProducerConfig) { config.MaxBufferedRecords = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := valid
			test.change(&config)
			if _, err := NewProducer(config); err == nil {
				t.Fatal("NewProducer() error = nil, want validation error")
			}
		})
	}
}

func TestProducerPublishValidatesMessage(t *testing.T) {
	t.Parallel()
	producer := &Producer{}
	tests := []outbox.Message{
		{Value: []byte(`{}`)},
		{Key: "transfer:id"},
	}
	for _, message := range tests {
		if err := producer.Publish(t.Context(), message); err == nil {
			t.Fatalf("Publish(%+v) error = nil, want validation error", message)
		}
	}
}

func TestValidKafkaName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value string
		want  bool
	}{
		{value: "wallet.events.v1", want: true},
		{value: "wallet_events-1", want: true},
		{value: "wallet events", want: false},
		{value: "", want: false},
	}
	for _, test := range tests {
		if got := validKafkaName(test.value, maxTopicBytes); got != test.want {
			t.Errorf("validKafkaName(%q) = %t, want %t", test.value, got, test.want)
		}
	}
}

func validProducerConfig() ProducerConfig {
	return ProducerConfig{
		Brokers:            []string{"localhost:9092"},
		Topic:              "wallet.events.v1",
		ClientID:           "intelligent-wallet-ledger-outbox",
		DeliveryTimeout:    10 * time.Second,
		MaxBufferedRecords: 100,
	}
}
