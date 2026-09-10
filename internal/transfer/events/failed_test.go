package events_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
	transferevents "github.com/krav01/intelligent-wallet-ledger/internal/transfer/events"
)

func TestFailed(t *testing.T) {
	t.Parallel()
	approved, err := mustPendingLifecycle(t).Approve()
	if err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	lifecycle, err := approved.Fail("insufficient_funds")
	if err != nil {
		t.Fatalf("Fail() error = %v", err)
	}
	failedAt := time.Date(2026, time.September, 11, 12, 2, 0, 0, time.UTC)
	causationID := "66666666-6666-4666-8666-666666666666"

	draft, err := transferevents.Failed(lifecycle, failedAt, causationID)
	if err != nil {
		t.Fatalf("Failed() error = %v", err)
	}
	if draft.EventType() != transferevents.FailedType ||
		draft.EventVersion() != transferevents.FailedVersion ||
		draft.AggregateID() != lifecycle.Transfer().ID() ||
		draft.AggregateVersion() != lifecycle.Version() ||
		draft.CorrelationID() != lifecycle.Transfer().ID() ||
		draft.CausationID() != causationID || !draft.OccurredAt().Equal(failedAt) {
		t.Errorf("Failed() metadata does not match lifecycle")
	}

	var payload struct {
		TransferID string `json:"transfer_id"`
		ReasonCode string `json:"reason_code"`
	}
	if err := json.Unmarshal(draft.Payload(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if payload.TransferID != lifecycle.Transfer().ID() || payload.ReasonCode != "insufficient_funds" {
		t.Errorf("Failed() payload = %+v, want terminal failure", payload)
	}
}

func TestFailedRejectsNonFailedLifecycle(t *testing.T) {
	t.Parallel()
	lifecycle, err := mustPendingLifecycle(t).Approve()
	if err != nil {
		t.Fatalf("Approve() error = %v", err)
	}

	_, err = transferevents.Failed(
		lifecycle,
		time.Date(2026, time.September, 11, 12, 2, 0, 0, time.UTC),
		"66666666-6666-4666-8666-666666666666",
	)
	if !errors.Is(err, transferdomain.ErrInvalidLifecycle) {
		t.Fatalf("Failed() error = %v, want ErrInvalidLifecycle", err)
	}
}

func TestFailedRejectsMissingCausationID(t *testing.T) {
	t.Parallel()
	approved, err := mustPendingLifecycle(t).Approve()
	if err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	lifecycle, err := approved.Fail("insufficient_funds")
	if err != nil {
		t.Fatalf("Fail() error = %v", err)
	}

	_, err = transferevents.Failed(
		lifecycle,
		time.Date(2026, time.September, 11, 12, 2, 0, 0, time.UTC),
		"",
	)
	if !errors.Is(err, transferdomain.ErrInvalidLifecycle) {
		t.Fatalf("Failed() error = %v, want ErrInvalidLifecycle", err)
	}
}
