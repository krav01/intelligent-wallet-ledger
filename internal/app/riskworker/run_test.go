package riskworker

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
	transferevents "github.com/krav01/intelligent-wallet-ledger/internal/transfer/events"
)

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", " postgres://wallet.example/db ")
	t.Setenv("KAFKA_BROKERS", " kafka-a:9092, kafka-b:9092 ")
	t.Setenv("KAFKA_TOPIC", "")
	t.Setenv("KAFKA_GROUP_ID", "")
	t.Setenv("RISK_POLICY_VERSION", "risk-v1")
	t.Setenv("RISK_REVIEW_AMOUNT_USD_MINOR", "50")
	t.Setenv("RISK_DECLINE_AMOUNT_USD_MINOR", "100")

	config, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if config.DatabaseURL != "postgres://wallet.example/db" || len(config.KafkaBrokers) != 2 ||
		config.KafkaBrokers[0] != "kafka-a:9092" || config.KafkaBrokers[1] != "kafka-b:9092" ||
		config.KafkaTopic != defaultTopic || config.KafkaGroupID != defaultGroupID || len(config.Policies) != 1 || config.Policies[0].Version() != "risk-v1" {
		t.Errorf("ConfigFromEnv() = %+v, want normalized configuration", config)
	}
}

func TestConfigFromEnvLoadsPolicyRegistry(t *testing.T) {
	t.Setenv("RISK_POLICIES_JSON", `[{"version":"risk-v1","review_amount_usd_minor":50,"decline_amount_usd_minor":100},{"version":"risk-v2","review_amount_usd_minor":75,"decline_amount_usd_minor":150}]`)
	config, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if len(config.Policies) != 2 || config.Policies[0].Version() != "risk-v1" || config.Policies[1].Version() != "risk-v2" {
		t.Errorf("ConfigFromEnv() policies = %+v, want risk-v1 and risk-v2", config.Policies)
	}
}

func TestConfigFromEnvRejectsMissingPolicyThreshold(t *testing.T) {
	t.Setenv("RISK_POLICY_VERSION", "risk-v1")
	t.Setenv("RISK_REVIEW_AMOUNT_USD_MINOR", "50")
	t.Setenv("RISK_DECLINE_AMOUNT_USD_MINOR", "")
	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("ConfigFromEnv() error = nil, want missing threshold error")
	}
}

func TestRunValidatesRequiredConfiguration(t *testing.T) {
	t.Parallel()
	if err := Run(t.Context(), Config{}, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("Run() error = nil, want missing database error")
	}
}

func TestHandleRecord(t *testing.T) {
	envelope := requestedEnvelope(t)
	encoded, err := envelope.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON() error = %v", err)
	}
	handler := &recordHandler{}

	got, err := handleRecord(t.Context(), handler, encoded)
	if err != nil {
		t.Fatalf("handleRecord() error = %v", err)
	}
	if got.EventID() != envelope.EventID() || handler.envelope.EventID() != envelope.EventID() {
		t.Errorf("handleRecord() event ID = %q, want %q", got.EventID(), envelope.EventID())
	}
}

func TestHandleRecordDoesNotCallHandlerForInvalidEnvelope(t *testing.T) {
	handler := &recordHandler{}
	if _, err := handleRecord(t.Context(), handler, []byte(`{"event_id":"invalid"}`)); err == nil {
		t.Fatal("handleRecord() error = nil, want invalid envelope error")
	}
	if handler.called {
		t.Fatal("handler was called for invalid envelope")
	}
}

func TestHandleRecordPropagatesHandlerError(t *testing.T) {
	envelope := requestedEnvelope(t)
	encoded, err := envelope.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON() error = %v", err)
	}
	want := errors.New("database unavailable")
	handler := &recordHandler{err: want}
	if _, err := handleRecord(t.Context(), handler, encoded); !errors.Is(err, want) {
		t.Fatalf("handleRecord() error = %v, want handler error", err)
	}
}

type recordHandler struct {
	envelope event.Envelope
	err      error
	called   bool
}

func (h *recordHandler) Handle(_ context.Context, envelope event.Envelope) error {
	h.called = true
	h.envelope = envelope
	return h.err
}

func requestedEnvelope(t *testing.T) event.Envelope {
	t.Helper()
	currency, err := ledgerdomain.ParseCurrency("USD")
	if err != nil {
		t.Fatalf("ParseCurrency() error = %v", err)
	}
	money, err := ledgerdomain.NewMoney(60, currency)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	transfer, err := transferdomain.NewTransfer(transferdomain.NewTransferParams{
		ID:                   "11111111-1111-4111-8111-111111111111",
		IdempotencyKey:       "request",
		RequesterID:          "22222222-2222-4222-8222-222222222222",
		SourceAccountID:      "33333333-3333-4333-8333-333333333333",
		DestinationAccountID: "44444444-4444-4444-8444-444444444444",
		Amount:               money,
		RequestedAt:          time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("NewTransfer() error = %v", err)
	}
	lifecycle, err := transferdomain.NewPendingLifecycle(transfer, "risk-v1")
	if err != nil {
		t.Fatalf("NewPendingLifecycle() error = %v", err)
	}
	draft, err := transferevents.Requested(lifecycle)
	if err != nil {
		t.Fatalf("Requested() error = %v", err)
	}
	envelope, err := event.NewEnvelope("55555555-5555-4555-8555-555555555555", draft)
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}
	return envelope
}
