package domain_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
)

func TestNewJournalEntry(t *testing.T) {
	t.Parallel()

	recordedAt := time.Date(2026, time.September, 10, 12, 30, 0, 0, time.FixedZone("UTC+3", 3*60*60))
	postings := balancedPostings(t, "USD", 125)
	entry, err := domain.NewJournalEntry(domain.NewJournalEntryParams{
		ID:         "  entry-1  ",
		Postings:   postings,
		RecordedAt: recordedAt,
	})
	if err != nil {
		t.Fatalf("NewJournalEntry() error = %v", err)
	}

	if got := entry.ID(); got != "entry-1" {
		t.Errorf("JournalEntry.ID() = %q, want entry-1", got)
	}
	if got := entry.Type(); got != domain.JournalEntryTypeStandard {
		t.Errorf("JournalEntry.Type() = %d, want JournalEntryTypeStandard", got)
	}
	if got := entry.ReversesEntryID(); got != "" {
		t.Errorf("JournalEntry.ReversesEntryID() = %q, want empty", got)
	}
	if got := entry.RecordedAt(); !got.Equal(recordedAt) || got.Location() != time.UTC {
		t.Errorf("JournalEntry.RecordedAt() = %v (%v), want same instant in UTC", got, got.Location())
	}
	if got := entry.Postings(); len(got) != len(postings) {
		t.Fatalf("len(JournalEntry.Postings()) = %d, want %d", len(got), len(postings))
	}
}

func TestNewJournalEntryBalancesEachCurrency(t *testing.T) {
	t.Parallel()

	postings := append(
		balancedPostings(t, "USD", 125),
		balancedPostings(t, "EUR", 80)...,
	)
	_, err := domain.NewJournalEntry(domain.NewJournalEntryParams{
		ID:         "entry-multi-currency",
		Postings:   postings,
		RecordedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("NewJournalEntry() error = %v", err)
	}
}

func TestNewJournalEntryRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	validPostings := balancedPostings(t, "USD", 125)
	tests := []struct {
		name   string
		params domain.NewJournalEntryParams
		want   error
	}{
		{
			name: "missing ID",
			params: domain.NewJournalEntryParams{
				Postings:   validPostings,
				RecordedAt: time.Now(),
			},
			want: domain.ErrInvalidJournalEntry,
		},
		{
			name: "missing recorded time",
			params: domain.NewJournalEntryParams{
				ID:       "entry-1",
				Postings: validPostings,
			},
			want: domain.ErrInvalidJournalEntry,
		},
		{
			name: "too few postings",
			params: domain.NewJournalEntryParams{
				ID:         "entry-1",
				Postings:   validPostings[:1],
				RecordedAt: time.Now(),
			},
			want: domain.ErrInvalidJournalEntry,
		},
		{
			name: "too many postings",
			params: domain.NewJournalEntryParams{
				ID:         "entry-1",
				Postings:   make([]domain.Posting, 1025),
				RecordedAt: time.Now(),
			},
			want: domain.ErrInvalidJournalEntry,
		},
		{
			name: "invalid posting zero value",
			params: domain.NewJournalEntryParams{
				ID:         "entry-1",
				Postings:   []domain.Posting{validPostings[0], {}},
				RecordedAt: time.Now(),
			},
			want: domain.ErrInvalidPosting,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := domain.NewJournalEntry(test.params)
			if !errors.Is(err, test.want) {
				t.Fatalf("NewJournalEntry() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestNewJournalEntryRejectsCrossCurrencyNetting(t *testing.T) {
	t.Parallel()

	postings := []domain.Posting{
		mustPosting(t, "account-usd", 100, mustCurrency(t, "USD")),
		mustPosting(t, "account-eur", -100, mustCurrency(t, "EUR")),
	}
	_, err := domain.NewJournalEntry(domain.NewJournalEntryParams{
		ID:         "entry-1",
		Postings:   postings,
		RecordedAt: time.Now(),
	})
	if !errors.Is(err, domain.ErrUnbalancedJournalEntry) {
		t.Fatalf("NewJournalEntry() error = %v, want ErrUnbalancedJournalEntry", err)
	}
}

func TestNewJournalEntryBalancesWithoutIntermediateOverflow(t *testing.T) {
	t.Parallel()

	usd := mustCurrency(t, "USD")
	postings := []domain.Posting{
		mustPosting(t, "account-1", math.MaxInt64, usd),
		mustPosting(t, "account-2", 1, usd),
		mustPosting(t, "account-3", -math.MaxInt64, usd),
		mustPosting(t, "account-4", -1, usd),
	}
	_, err := domain.NewJournalEntry(domain.NewJournalEntryParams{
		ID:         "entry-extremes",
		Postings:   postings,
		RecordedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("NewJournalEntry() error = %v", err)
	}
}

func TestJournalEntryDefensivelyCopiesPostings(t *testing.T) {
	t.Parallel()

	postings := balancedPostings(t, "USD", 125)
	entry, err := domain.NewJournalEntry(domain.NewJournalEntryParams{
		ID:         "entry-1",
		Postings:   postings,
		RecordedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("NewJournalEntry() error = %v", err)
	}

	originalFirst := entry.Postings()[0]
	postings[0] = postings[1]
	returned := entry.Postings()
	returned[0] = returned[1]

	if got := entry.Postings()[0]; got != originalFirst {
		t.Errorf("JournalEntry.Postings()[0] = %+v, want immutable %+v", got, originalFirst)
	}
}

func TestJournalEntryReverse(t *testing.T) {
	t.Parallel()

	postings := balancedPostings(t, "USD", 125)
	entry := mustJournalEntry(t, "entry-1", postings)
	recordedAt := time.Date(2026, time.September, 10, 15, 0, 0, 0, time.UTC)

	reversal, err := entry.Reverse("reversal-1", recordedAt)
	if err != nil {
		t.Fatalf("JournalEntry.Reverse() error = %v", err)
	}
	if got := reversal.Type(); got != domain.JournalEntryTypeReversal {
		t.Errorf("JournalEntry.Type() = %d, want JournalEntryTypeReversal", got)
	}
	if got := reversal.ReversesEntryID(); got != entry.ID() {
		t.Errorf("JournalEntry.ReversesEntryID() = %q, want %q", got, entry.ID())
	}
	if got := reversal.RecordedAt(); !got.Equal(recordedAt) {
		t.Errorf("JournalEntry.RecordedAt() = %v, want %v", got, recordedAt)
	}

	reversedPostings := reversal.Postings()
	for index, original := range entry.Postings() {
		got := reversedPostings[index]
		if got.AccountID() != original.AccountID() {
			t.Errorf("reversal posting %d account = %q, want %q", index, got.AccountID(), original.AccountID())
		}
		if got.Amount().MinorUnits() != -original.Amount().MinorUnits() {
			t.Errorf(
				"reversal posting %d amount = %d, want %d",
				index,
				got.Amount().MinorUnits(),
				-original.Amount().MinorUnits(),
			)
		}
	}
}

func TestJournalEntryReverseRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	entry := mustJournalEntry(t, "entry-1", balancedPostings(t, "USD", 125))
	tests := []struct {
		name       string
		entry      domain.JournalEntry
		newID      string
		recordedAt time.Time
		want       error
	}{
		{
			name:       "same ID",
			entry:      entry,
			newID:      entry.ID(),
			recordedAt: time.Now(),
			want:       domain.ErrInvalidJournalEntry,
		},
		{
			name:       "missing new ID",
			entry:      entry,
			recordedAt: time.Now(),
			want:       domain.ErrInvalidJournalEntry,
		},
		{
			name:  "missing recorded time",
			entry: entry,
			newID: "reversal-1",
			want:  domain.ErrInvalidJournalEntry,
		},
		{
			name:       "invalid source zero value",
			entry:      domain.JournalEntry{},
			newID:      "reversal-1",
			recordedAt: time.Now(),
			want:       domain.ErrInvalidJournalEntry,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := test.entry.Reverse(test.newID, test.recordedAt)
			if !errors.Is(err, test.want) {
				t.Fatalf("JournalEntry.Reverse() error = %v, want %v", err, test.want)
			}
		})
	}
}

func balancedPostings(t *testing.T, currencyCode string, amount int64) []domain.Posting {
	t.Helper()

	currency := mustCurrency(t, currencyCode)
	return []domain.Posting{
		mustPosting(t, "account-1", amount, currency),
		mustPosting(t, "account-2", -amount, currency),
	}
}

func mustPosting(t *testing.T, accountID string, minorUnits int64, currency domain.Currency) domain.Posting {
	t.Helper()

	posting, err := domain.NewPosting(accountID, mustMoney(t, minorUnits, currency))
	if err != nil {
		t.Fatalf("NewPosting() error = %v", err)
	}

	return posting
}

func mustJournalEntry(t *testing.T, id string, postings []domain.Posting) domain.JournalEntry {
	t.Helper()

	entry, err := domain.NewJournalEntry(domain.NewJournalEntryParams{
		ID:         id,
		Postings:   postings,
		RecordedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("NewJournalEntry() error = %v", err)
	}

	return entry
}
