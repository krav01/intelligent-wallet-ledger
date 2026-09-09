package domain_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
)

func TestNewTransfer(t *testing.T) {
	t.Parallel()

	requestedAt := time.Date(2026, time.September, 10, 12, 30, 0, 0, time.FixedZone("test", 3*60*60))
	amount := mustMoney(t, 125, "USD")
	transfer, err := transferdomain.NewTransfer(transferdomain.NewTransferParams{
		ID:                   " 11111111-1111-4111-8111-11111111111A ",
		IdempotencyKey:       " request-1 ",
		RequesterID:          " 22222222-2222-4222-8222-22222222222B ",
		SourceAccountID:      " 33333333-3333-4333-8333-33333333333C ",
		DestinationAccountID: " 44444444-4444-4444-8444-44444444444D ",
		Amount:               amount,
		RequestedAt:          requestedAt,
	})
	if err != nil {
		t.Fatalf("NewTransfer() error = %v", err)
	}

	if transfer.ID() != "11111111-1111-4111-8111-11111111111a" ||
		transfer.IdempotencyKey() != "request-1" ||
		transfer.RequesterID() != "22222222-2222-4222-8222-22222222222b" ||
		transfer.SourceAccountID() != "33333333-3333-4333-8333-33333333333c" ||
		transfer.DestinationAccountID() != "44444444-4444-4444-8444-44444444444d" ||
		transfer.Amount() != amount {
		t.Errorf("NewTransfer() = %+v, want normalized immutable values", transfer)
	}
	if got := transfer.RequestedAt(); !got.Equal(requestedAt) || got.Location() != time.UTC {
		t.Errorf("Transfer.RequestedAt() = %v (%v), want same instant in UTC", got, got.Location())
	}
}

func TestNewTransferRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	valid := validParams(t)
	zeroAmount := mustMoney(t, 0, "USD")
	negativeAmount := mustMoney(t, -1, "USD")
	tests := []struct {
		name   string
		change func(*transferdomain.NewTransferParams)
	}{
		{name: "missing ID", change: func(params *transferdomain.NewTransferParams) { params.ID = " " }},
		{
			name:   "malformed ID",
			change: func(params *transferdomain.NewTransferParams) { params.ID = "transfer-1" },
		},
		{
			name:   "missing idempotency key",
			change: func(params *transferdomain.NewTransferParams) { params.IdempotencyKey = " " },
		},
		{
			name: "idempotency key is too long",
			change: func(params *transferdomain.NewTransferParams) {
				params.IdempotencyKey = strings.Repeat("a", 129)
			},
		},
		{
			name: "idempotency key contains whitespace",
			change: func(params *transferdomain.NewTransferParams) {
				params.IdempotencyKey = "request 1"
			},
		},
		{
			name: "idempotency key is not ASCII",
			change: func(params *transferdomain.NewTransferParams) {
				params.IdempotencyKey = "запрос-1"
			},
		},
		{
			name:   "missing requester ID",
			change: func(params *transferdomain.NewTransferParams) { params.RequesterID = " " },
		},
		{
			name:   "missing source account ID",
			change: func(params *transferdomain.NewTransferParams) { params.SourceAccountID = " " },
		},
		{
			name: "missing destination account ID",
			change: func(params *transferdomain.NewTransferParams) {
				params.DestinationAccountID = " "
			},
		},
		{
			name: "same account",
			change: func(params *transferdomain.NewTransferParams) {
				params.DestinationAccountID = strings.ToUpper(params.SourceAccountID)
			},
		},
		{
			name: "invalid amount currency",
			change: func(params *transferdomain.NewTransferParams) {
				params.Amount = ledgerdomain.Money{}
			},
		},
		{
			name: "zero amount",
			change: func(params *transferdomain.NewTransferParams) {
				params.Amount = zeroAmount
			},
		},
		{
			name: "negative amount",
			change: func(params *transferdomain.NewTransferParams) {
				params.Amount = negativeAmount
			},
		},
		{
			name:   "missing requested time",
			change: func(params *transferdomain.NewTransferParams) { params.RequestedAt = time.Time{} },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			params := valid
			test.change(&params)
			_, err := transferdomain.NewTransfer(params)
			if !errors.Is(err, transferdomain.ErrInvalidTransfer) {
				t.Fatalf("NewTransfer() error = %v, want ErrInvalidTransfer", err)
			}
		})
	}
}

func TestTransferJournalEntry(t *testing.T) {
	t.Parallel()

	params := validParams(t)
	transfer, err := transferdomain.NewTransfer(params)
	if err != nil {
		t.Fatalf("NewTransfer() error = %v", err)
	}

	entry, err := transfer.JournalEntry()
	if err != nil {
		t.Fatalf("Transfer.JournalEntry() error = %v", err)
	}
	if entry.ID() != transfer.ID() ||
		entry.Type() != ledgerdomain.JournalEntryTypeStandard ||
		!entry.RecordedAt().Equal(transfer.RequestedAt()) {
		t.Errorf("Transfer.JournalEntry() = %+v, want transfer identity and time", entry)
	}
	postings := entry.Postings()
	if len(postings) != 2 {
		t.Fatalf("len(JournalEntry.Postings()) = %d, want 2", len(postings))
	}
	if postings[0].AccountID() != transfer.SourceAccountID() ||
		postings[0].Amount().MinorUnits() != -transfer.Amount().MinorUnits() ||
		postings[0].Amount().Currency() != transfer.Amount().Currency() {
		t.Errorf("source posting = %+v, want debit from source account", postings[0])
	}
	if postings[1].AccountID() != transfer.DestinationAccountID() ||
		postings[1].Amount() != transfer.Amount() {
		t.Errorf("destination posting = %+v, want credit to destination account", postings[1])
	}
}

func TestZeroTransferCannotCreateJournalEntry(t *testing.T) {
	t.Parallel()

	_, err := (transferdomain.Transfer{}).JournalEntry()
	if !errors.Is(err, transferdomain.ErrInvalidTransfer) {
		t.Fatalf("Transfer{}.JournalEntry() error = %v, want ErrInvalidTransfer", err)
	}
}

func validParams(t testing.TB) transferdomain.NewTransferParams {
	t.Helper()

	return transferdomain.NewTransferParams{
		ID:                   "11111111-1111-4111-8111-111111111111",
		IdempotencyKey:       "request-1",
		RequesterID:          "22222222-2222-4222-8222-222222222222",
		SourceAccountID:      "33333333-3333-4333-8333-33333333333a",
		DestinationAccountID: "44444444-4444-4444-8444-44444444444b",
		Amount:               mustMoney(t, 125, "USD"),
		RequestedAt:          time.Date(2026, time.September, 10, 9, 30, 0, 0, time.UTC),
	}
}

func mustMoney(t testing.TB, minorUnits int64, currencyCode string) ledgerdomain.Money {
	t.Helper()

	currency, err := ledgerdomain.ParseCurrency(currencyCode)
	if err != nil {
		t.Fatalf("ParseCurrency(%q) error = %v", currencyCode, err)
	}
	amount, err := ledgerdomain.NewMoney(minorUnits, currency)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}

	return amount
}
