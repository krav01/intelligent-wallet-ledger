package transactionworker

import (
	"errors"
	"testing"
	"time"

	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
)

func TestJournalEntry(t *testing.T) {
	t.Parallel()
	lifecycle := mustApprovedLifecycle(t)
	recordedAt := time.Date(2026, time.September, 12, 12, 0, 0, 0, time.UTC)

	entry, err := journalEntry(lifecycle, recordedAt)
	if err != nil {
		t.Fatalf("journalEntry() error = %v", err)
	}
	if entry.ID() != lifecycle.Transfer().ID() || !entry.RecordedAt().Equal(recordedAt) {
		t.Errorf("journal entry header does not preserve transfer identity and recording time")
	}
	postings := entry.Postings()
	if len(postings) != 2 || postings[0].AccountID() != lifecycle.Transfer().SourceAccountID() ||
		postings[0].Amount().MinorUnits() != -50 || postings[1].AccountID() != lifecycle.Transfer().DestinationAccountID() ||
		postings[1].Amount().MinorUnits() != 50 {
		t.Errorf("journal entry postings = %+v, want balanced source debit and destination credit", postings)
	}
}

func TestJournalEntryRejectsNonApprovedLifecycle(t *testing.T) {
	t.Parallel()
	pending := mustPendingLifecycle(t)
	_, err := journalEntry(pending, time.Now())
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("journalEntry() error = %v, want ErrInvalidArgument", err)
	}
}

func mustApprovedLifecycle(t testing.TB) transferdomain.Lifecycle {
	t.Helper()
	lifecycle, err := mustPendingLifecycle(t).Approve()
	if err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	return lifecycle
}

func mustPendingLifecycle(t testing.TB) transferdomain.Lifecycle {
	t.Helper()
	currency, err := ledgerdomain.ParseCurrency("USD")
	if err != nil {
		t.Fatal(err)
	}
	money, err := ledgerdomain.NewMoney(50, currency)
	if err != nil {
		t.Fatal(err)
	}
	transfer, err := transferdomain.NewTransfer(transferdomain.NewTransferParams{
		ID:                   "11111111-1111-4111-8111-111111111111",
		IdempotencyKey:       "transaction-worker",
		RequesterID:          "22222222-2222-4222-8222-222222222222",
		SourceAccountID:      "33333333-3333-4333-8333-333333333333",
		DestinationAccountID: "44444444-4444-4444-8444-444444444444",
		Amount:               money,
		RequestedAt:          time.Date(2026, time.September, 12, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	lifecycle, err := transferdomain.NewPendingLifecycle(transfer, "risk-v1")
	if err != nil {
		t.Fatal(err)
	}
	return lifecycle
}
