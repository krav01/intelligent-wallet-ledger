package events_test

import (
	"testing"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	transferevents "github.com/krav01/intelligent-wallet-ledger/internal/transfer/events"
)

func TestReviewDecided(t *testing.T) {
	review, err := mustPendingLifecycle(t).RequireReview()
	if err != nil {
		t.Fatal(err)
	}
	approved, err := review.Approve()
	if err != nil {
		t.Fatal(err)
	}
	draft, err := transferevents.ReviewDecided(approved, "analyst-1", time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := event.NewEnvelope("66666666-6666-4666-8666-666666666666", draft)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := transferevents.ParseReviewDecided(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.TransferID() != approved.Transfer().ID() || parsed.Decision() != "approved" {
		t.Errorf("parsed = (%q, %q), want approved lifecycle", parsed.TransferID(), parsed.Decision())
	}
}
