package events_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
	transferevents "github.com/krav01/intelligent-wallet-ledger/internal/transfer/events"
)

func TestCompleted(t *testing.T) {
	t.Parallel()
	transfer := mustTransfer(t)

	draft, err := transferevents.Completed(transfer)
	if err != nil {
		t.Fatalf("Completed() error = %v", err)
	}
	if draft.EventType() != transferevents.CompletedType ||
		draft.EventVersion() != transferevents.CompletedVersion ||
		draft.AggregateType() != "transfer" ||
		draft.AggregateID() != transfer.ID() ||
		draft.AggregateVersion() != 1 ||
		draft.CorrelationID() != transfer.ID() ||
		draft.CausationID() != "" ||
		!draft.OccurredAt().Equal(transfer.RequestedAt()) {
		t.Errorf("Completed() metadata does not match transfer")
	}
	var payload struct {
		TransferID           string `json:"transfer_id"`
		RequesterID          string `json:"requester_id"`
		SourceAccountID      string `json:"source_account_id"`
		DestinationAccountID string `json:"destination_account_id"`
		Currency             string `json:"currency"`
		AmountMinor          int64  `json:"amount_minor"`
		RequestedAt          string `json:"requested_at"`
	}
	if err := json.Unmarshal(draft.Payload(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if payload.TransferID != transfer.ID() || payload.RequesterID != transfer.RequesterID() ||
		payload.SourceAccountID != transfer.SourceAccountID() ||
		payload.DestinationAccountID != transfer.DestinationAccountID() ||
		payload.Currency != "USD" || payload.AmountMinor != 125 {
		t.Errorf("Completed() payload = %+v, want canonical transfer intent", payload)
	}
}

func TestCompletedRejectsZeroTransfer(t *testing.T) {
	t.Parallel()
	_, err := transferevents.Completed(transferdomain.Transfer{})
	if !errors.Is(err, transferdomain.ErrInvalidTransfer) {
		t.Fatalf("Completed() error = %v, want ErrInvalidTransfer", err)
	}
}

func mustTransfer(t testing.TB) transferdomain.Transfer {
	t.Helper()
	currency, err := ledgerdomain.ParseCurrency("USD")
	if err != nil {
		t.Fatalf("ParseCurrency() error = %v", err)
	}
	amount, err := ledgerdomain.NewMoney(125, currency)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	transfer, err := transferdomain.NewTransfer(transferdomain.NewTransferParams{
		ID:                   "11111111-1111-4111-8111-111111111111",
		IdempotencyKey:       "request-1",
		RequesterID:          "22222222-2222-4222-8222-222222222222",
		SourceAccountID:      "33333333-3333-4333-8333-333333333333",
		DestinationAccountID: "44444444-4444-4444-8444-444444444444",
		Amount:               amount,
		RequestedAt:          time.Date(2026, time.September, 10, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("NewTransfer() error = %v", err)
	}
	return transfer
}
