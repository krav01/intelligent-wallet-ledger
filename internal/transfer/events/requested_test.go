package events_test

import (
	"encoding/json"
	"errors"
	"testing"

	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
	transferevents "github.com/krav01/intelligent-wallet-ledger/internal/transfer/events"
)

func TestRequested(t *testing.T) {
	t.Parallel()
	pending := mustPendingLifecycle(t)

	draft, err := transferevents.Requested(pending)
	if err != nil {
		t.Fatalf("Requested() error = %v", err)
	}
	if draft.EventType() != transferevents.RequestedType ||
		draft.EventVersion() != transferevents.RequestedVersion ||
		draft.AggregateType() != "transfer" ||
		draft.AggregateID() != pending.Transfer().ID() ||
		draft.AggregateVersion() != pending.Version() ||
		draft.CorrelationID() != pending.Transfer().ID() ||
		draft.CausationID() != "" ||
		!draft.OccurredAt().Equal(pending.Transfer().RequestedAt()) {
		t.Errorf("Requested() metadata does not match lifecycle")
	}

	var payload struct {
		TransferID           string `json:"transfer_id"`
		RequesterID          string `json:"requester_id"`
		SourceAccountID      string `json:"source_account_id"`
		DestinationAccountID string `json:"destination_account_id"`
		Currency             string `json:"currency"`
		AmountMinor          int64  `json:"amount_minor"`
		RiskPolicyVersion    string `json:"risk_policy_version"`
	}
	if err := json.Unmarshal(draft.Payload(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	transfer := pending.Transfer()
	if payload.TransferID != transfer.ID() || payload.RequesterID != transfer.RequesterID() ||
		payload.SourceAccountID != transfer.SourceAccountID() ||
		payload.DestinationAccountID != transfer.DestinationAccountID() ||
		payload.Currency != "USD" || payload.AmountMinor != 125 ||
		payload.RiskPolicyVersion != pending.RiskPolicyVersion() {
		t.Errorf("Requested() payload = %+v, want canonical pending intent", payload)
	}
}

func TestRequestedRejectsNonPendingLifecycle(t *testing.T) {
	t.Parallel()
	approved, err := mustPendingLifecycle(t).Approve()
	if err != nil {
		t.Fatalf("Approve() error = %v", err)
	}

	_, err = transferevents.Requested(approved)
	if !errors.Is(err, transferdomain.ErrInvalidLifecycle) {
		t.Fatalf("Requested() error = %v, want ErrInvalidLifecycle", err)
	}
}

func mustPendingLifecycle(t testing.TB) transferdomain.Lifecycle {
	t.Helper()
	lifecycle, err := transferdomain.NewPendingLifecycle(mustTransfer(t), "risk-v1")
	if err != nil {
		t.Fatalf("NewPendingLifecycle() error = %v", err)
	}
	return lifecycle
}
