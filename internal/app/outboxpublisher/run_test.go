package outboxpublisher

import (
	"log/slog"
	"testing"
)

func TestConfigFromEnv(t *testing.T) {
	t.Run("loads defaults and broker list", func(t *testing.T) {
		t.Setenv("DATABASE_URL", " postgres://wallet.example/db ")
		t.Setenv("KAFKA_BROKERS", " kafka-a:9092, kafka-b:9092 ")
		t.Setenv("KAFKA_TOPIC", "")
		t.Setenv("KAFKA_ALLOW_AUTO_TOPIC_CREATION", "")

		config, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("ConfigFromEnv() error = %v", err)
		}
		if config.DatabaseURL != "postgres://wallet.example/db" ||
			len(config.KafkaBrokers) != 2 ||
			config.KafkaBrokers[0] != "kafka-a:9092" ||
			config.KafkaBrokers[1] != "kafka-b:9092" ||
			config.KafkaTopic != defaultTopic ||
			config.AllowAutoTopicCreation {
			t.Errorf("ConfigFromEnv() = %+v, want normalized defaults", config)
		}
	})

	t.Run("rejects malformed boolean", func(t *testing.T) {
		t.Setenv("KAFKA_ALLOW_AUTO_TOPIC_CREATION", "sometimes")
		if _, err := ConfigFromEnv(); err == nil {
			t.Fatal("ConfigFromEnv() error = nil, want boolean parse error")
		}
	})
}

func TestRunValidatesRequiredConfiguration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		config Config
		logger *slog.Logger
	}{
		{name: "missing logger", config: Config{DatabaseURL: "postgres://db", KafkaBrokers: []string{"kafka:9092"}}},
		{name: "missing database", config: Config{KafkaBrokers: []string{"kafka:9092"}}, logger: slog.New(slog.DiscardHandler)},
		{name: "missing brokers", config: Config{DatabaseURL: "postgres://db"}, logger: slog.New(slog.DiscardHandler)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := Run(t.Context(), test.config, test.logger); err == nil {
				t.Fatal("Run() error = nil, want validation error")
			}
		})
	}
}
