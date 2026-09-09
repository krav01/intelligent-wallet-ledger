package postgres

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
)

const (
	accountA = "00000000-0000-0000-0000-00000000000a"
	accountB = "00000000-0000-0000-0000-00000000000b"
	accountC = "00000000-0000-0000-0000-00000000000c"
	entryA   = "10000000-0000-0000-0000-00000000000a"
)

func TestNewRepositoryRejectsNilPool(t *testing.T) {
	t.Parallel()

	_, err := NewRepository(nil)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("NewRepository(nil) error = %v, want ErrInvalidArgument", err)
	}
}

func TestPrepareEntry(t *testing.T) {
	t.Parallel()

	entry := mustJournalEntry(t, entryA, []domain.Posting{
		mustPosting(t, accountB, -60, "USD"),
		mustPosting(t, accountA, 100, "USD"),
		mustPosting(t, accountA, -40, "USD"),
	})

	prepared, err := prepareEntry(entry)
	if err != nil {
		t.Fatalf("prepareEntry() error = %v", err)
	}
	if len(prepared.postings) != 3 {
		t.Fatalf("prepared posting count = %d, want 3", len(prepared.postings))
	}
	if len(prepared.changes) != 2 {
		t.Fatalf("prepared change count = %d, want 2", len(prepared.changes))
	}
	if got := prepared.changes[0].accountID.String(); got != accountA {
		t.Errorf("first change account = %q, want %q", got, accountA)
	}
	if got := prepared.changes[0].delta; got != 60 {
		t.Errorf("first change delta = %d, want 60", got)
	}
	if got := prepared.changes[1].accountID.String(); got != accountB {
		t.Errorf("second change account = %q, want %q", got, accountB)
	}
	if got := prepared.changes[1].delta; got != -60 {
		t.Errorf("second change delta = %d, want -60", got)
	}
}

func TestPrepareEntryRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	validPostings := []domain.Posting{
		mustPosting(t, accountA, 100, "USD"),
		mustPosting(t, accountB, -100, "USD"),
	}
	invalidEntryID := mustJournalEntry(t, "not-a-uuid", validPostings)
	invalidAccountID := mustJournalEntry(t, entryA, []domain.Posting{
		mustPosting(t, "not-a-uuid", 100, "USD"),
		mustPosting(t, accountB, -100, "USD"),
	})
	overflow := mustJournalEntry(t, entryA, []domain.Posting{
		mustPosting(t, accountA, math.MaxInt64, "USD"),
		mustPosting(t, accountA, 1, "USD"),
		mustPosting(t, accountB, -math.MaxInt64, "USD"),
		mustPosting(t, accountB, -1, "USD"),
	})

	tests := []struct {
		name  string
		entry domain.JournalEntry
		want  error
	}{
		{name: "zero value", entry: domain.JournalEntry{}, want: ErrInvalidArgument},
		{name: "invalid entry ID", entry: invalidEntryID, want: ErrInvalidArgument},
		{name: "invalid account ID", entry: invalidAccountID, want: ErrInvalidArgument},
		{name: "account aggregate overflow", entry: overflow, want: ErrAmountOverflow},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := prepareEntry(test.entry)
			if !errors.Is(err, test.want) {
				t.Fatalf("prepareEntry() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestPreparePostingsRejectsAccountCurrencyMismatch(t *testing.T) {
	t.Parallel()

	postings := []domain.Posting{
		mustPosting(t, accountA, 100, "USD"),
		mustPosting(t, accountA, 100, "EUR"),
	}
	_, _, err := preparePostings(postings)
	if !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("preparePostings() error = %v, want ErrCurrencyMismatch", err)
	}
}

func TestCheckedAdd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		left  int64
		right int64
		want  int64
		err   bool
	}{
		{name: "positive", left: 10, right: 5, want: 15},
		{name: "negative", left: 10, right: -5, want: 5},
		{name: "positive overflow", left: math.MaxInt64, right: 1, err: true},
		{name: "negative overflow", left: math.MinInt64, right: -1, err: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := checkedAdd(test.left, test.right)
			if (err != nil) != test.err {
				t.Fatalf("checkedAdd(%d, %d) error = %v, want error %t", test.left, test.right, err, test.err)
			}
			if got != test.want {
				t.Errorf("checkedAdd(%d, %d) = %d, want %d", test.left, test.right, got, test.want)
			}
		})
	}
}

func TestClassifyPostError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		code       string
		constraint string
		want       error
	}{
		{
			name:       "duplicate entry",
			code:       "23505",
			constraint: "journal_entries_pkey",
			want:       ErrAlreadyExists,
		},
		{
			name:       "second reversal",
			code:       "23505",
			constraint: "journal_entries_one_reversal_idx",
			want:       ErrAlreadyReversed,
		},
		{
			name:       "negative customer balance",
			code:       "23514",
			constraint: "account_balances_nonnegative_customer_check",
			want:       ErrInsufficientFunds,
		},
		{name: "numeric overflow", code: "22003", want: ErrAmountOverflow},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			postgresError := &pgconn.PgError{
				Code:           test.code,
				ConstraintName: test.constraint,
			}
			got := classifyPostError(fmtError(postgresError))
			if !errors.Is(got, test.want) {
				t.Fatalf("classifyPostError() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestIsRetryableTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code string
		want bool
	}{
		{name: "serialization failure", code: "40001", want: true},
		{name: "deadlock", code: "40P01", want: true},
		{name: "unique violation", code: "23505", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := isRetryableTransaction(fmtError(&pgconn.PgError{Code: test.code}))
			if got != test.want {
				t.Errorf("isRetryableTransaction() = %t, want %t", got, test.want)
			}
		})
	}
}

func fmtError(err error) error {
	return &wrappedError{err: err}
}

type wrappedError struct {
	err error
}

func (e *wrappedError) Error() string {
	return "wrapped: " + e.err.Error()
}

func (e *wrappedError) Unwrap() error {
	return e.err
}

func mustJournalEntry(t testing.TB, id string, postings []domain.Posting) domain.JournalEntry {
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

func mustPosting(t testing.TB, accountID string, minorUnits int64, currencyCode string) domain.Posting {
	t.Helper()

	currency, err := domain.ParseCurrency(currencyCode)
	if err != nil {
		t.Fatalf("ParseCurrency(%q) error = %v", currencyCode, err)
	}
	amount, err := domain.NewMoney(minorUnits, currency)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	posting, err := domain.NewPosting(accountID, amount)
	if err != nil {
		t.Fatalf("NewPosting() error = %v", err)
	}

	return posting
}
