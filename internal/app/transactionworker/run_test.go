package transactionworker

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	riskdomain "github.com/krav01/intelligent-wallet-ledger/internal/risk/domain"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
	transferevents "github.com/krav01/intelligent-wallet-ledger/internal/transfer/events"
)

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", " postgres://wallet.example/db ")
	t.Setenv("KAFKA_BROKERS", " kafka-a:9092, kafka-b:9092 ")
	t.Setenv("KAFKA_TOPIC", "")
	t.Setenv("KAFKA_TRANSACTION_GROUP_ID", "")

	config := ConfigFromEnv()
	if config.DatabaseURL != "postgres://wallet.example/db" || len(config.KafkaBrokers) != 2 ||
		config.KafkaBrokers[0] != "kafka-a:9092" || config.KafkaBrokers[1] != "kafka-b:9092" ||
		config.KafkaTopic != defaultTopic || config.KafkaGroupID != defaultGroupID {
		t.Errorf("ConfigFromEnv() = %+v, want normalized defaults", config)
	}
}

func TestRunValidatesRequiredConfiguration(t *testing.T) {
	t.Parallel()
	if err := Run(t.Context(), Config{}, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("Run() error = nil, want missing database error")
	}
}

func TestHandleRecord(t *testing.T) {
	envelope := approvedRiskEnvelope(t)
	encoded, err := envelope.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON() error = %v", err)
	}
	handler := &recordHandler{}

	got, handled, err := handleRecord(t.Context(), handler, encoded)
	if err != nil {
		t.Fatalf("handleRecord() error = %v", err)
	}
	if !handled {
		t.Fatal("handleRecord() handled = false, want true")
	}
	if got.EventID() != envelope.EventID() || handler.envelope.EventID() != envelope.EventID() {
		t.Errorf("handleRecord() event ID = %q, want %q", got.EventID(), envelope.EventID())
	}
}

func TestHandleRecordDoesNotCallHandlerForInvalidEnvelope(t *testing.T) {
	handler := &recordHandler{}
	if _, _, err := handleRecord(t.Context(), handler, []byte(`{"event_id":"invalid"}`)); err == nil {
		t.Fatal("handleRecord() error = nil, want invalid envelope error")
	}
	if handler.called {
		t.Fatal("handler was called for invalid envelope")
	}
}

func TestHandleRecordPropagatesHandlerError(t *testing.T) {
	envelope := approvedRiskEnvelope(t)
	encoded, err := envelope.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON() error = %v", err)
	}
	want := errors.New("database unavailable")
	handler := &recordHandler{err: want}
	if _, _, err := handleRecord(t.Context(), handler, encoded); !errors.Is(err, want) {
		t.Fatalf("handleRecord() error = %v, want handler error", err)
	}
}

func TestHandleRecordIgnoresKnownForeignEvent(t *testing.T) {
	envelope := knownForeignEnvelope(t, transferevents.RequestedType)
	encoded, err := envelope.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON() error = %v", err)
	}
	handler := &recordHandler{}

	_, handled, err := handleRecord(t.Context(), handler, encoded)
	if err != nil {
		t.Fatalf("handleRecord() error = %v", err)
	}
	if handled || handler.called {
		t.Fatal("known foreign event was handled, want ignored")
	}
}

func TestQuarantineReason(t *testing.T) {
	t.Parallel()
	if reason, ok := quarantineReason(event.ErrInvalidEnvelope); !ok || reason != "invalid_envelope" {
		t.Errorf("quarantineReason(invalid envelope) = (%q, %t), want (invalid_envelope, true)", reason, ok)
	}
	if reason, ok := quarantineReason(errUnsupportedEventType); !ok || reason != "unsupported_event_type" {
		t.Errorf("quarantineReason(unsupported type) = (%q, %t), want (unsupported_event_type, true)", reason, ok)
	}
	if _, ok := quarantineReason(errors.New("database unavailable")); ok {
		t.Error("quarantineReason(database error) = quarantinable, want false")
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

func approvedRiskEnvelope(t testing.TB) event.Envelope {
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
		RequestedAt:          time.Date(2026, time.September, 12, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("NewTransfer() error = %v", err)
	}
	pending, err := transferdomain.NewPendingLifecycle(transfer, "risk-v1")
	if err != nil {
		t.Fatalf("NewPendingLifecycle() error = %v", err)
	}
	approved, err := pending.Approve()
	if err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	policy, err := riskdomain.NewPolicy(riskdomain.PolicyParams{Version: "risk-v1", Thresholds: []riskdomain.Threshold{{Currency: currency, ReviewAmountMinor: 100, DeclineAmountMinor: 200}}})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	evaluation, err := policy.Evaluate(riskdomain.Input{Amount: money})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	draft, err := transferevents.RiskAssessed(approved, evaluation, time.Date(2026, time.September, 12, 1, 0, 0, 0, time.UTC), "55555555-5555-4555-8555-555555555555")
	if err != nil {
		t.Fatalf("RiskAssessed() error = %v", err)
	}
	envelope, err := event.NewEnvelope("66666666-6666-4666-8666-666666666666", draft)
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}
	return envelope
}

func knownForeignEnvelope(t testing.TB, eventType string) event.Envelope {
	t.Helper()
	draft, err := event.NewDraft(event.DraftParams{
		EventType:        eventType,
		EventVersion:     1,
		AggregateType:    "transfer",
		AggregateID:      "11111111-1111-4111-8111-111111111111",
		AggregateVersion: 1,
		CorrelationID:    "11111111-1111-4111-8111-111111111111",
		OccurredAt:       time.Date(2026, time.September, 15, 0, 0, 0, 0, time.UTC),
		Payload:          []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("NewDraft() error = %v", err)
	}
	envelope, err := event.NewEnvelope("33333333-3333-4333-8333-333333333333", draft)
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}
	return envelope
}
