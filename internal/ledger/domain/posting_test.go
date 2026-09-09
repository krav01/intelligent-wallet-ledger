package domain_test

import (
	"errors"
	"math"
	"testing"

	"github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
)

func TestNewPosting(t *testing.T) {
	t.Parallel()

	amount := mustMoney(t, -125, mustCurrency(t, "USD"))
	posting, err := domain.NewPosting("  account-1  ", amount)
	if err != nil {
		t.Fatalf("NewPosting() error = %v", err)
	}
	if got := posting.AccountID(); got != "account-1" {
		t.Errorf("Posting.AccountID() = %q, want account-1", got)
	}
	if got := posting.Amount(); got != amount {
		t.Errorf("Posting.Amount() = %+v, want %+v", got, amount)
	}
}

func TestNewPostingRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	usd := mustCurrency(t, "USD")
	tests := []struct {
		name      string
		accountID string
		amount    domain.Money
		want      error
	}{
		{
			name:      "missing account ID",
			accountID: "   ",
			amount:    mustMoney(t, 100, usd),
			want:      domain.ErrInvalidPosting,
		},
		{
			name:      "zero amount",
			accountID: "account-1",
			amount:    mustMoney(t, 0, usd),
			want:      domain.ErrInvalidPosting,
		},
		{
			name:      "invalid money",
			accountID: "account-1",
			amount:    domain.Money{},
			want:      domain.ErrInvalidCurrency,
		},
		{
			name:      "amount cannot be reversed",
			accountID: "account-1",
			amount:    mustMoney(t, math.MinInt64, usd),
			want:      domain.ErrAmountOverflow,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := domain.NewPosting(test.accountID, test.amount)
			if !errors.Is(err, test.want) {
				t.Fatalf("NewPosting() error = %v, want %v", err, test.want)
			}
			if !errors.Is(err, domain.ErrInvalidPosting) {
				t.Fatalf("NewPosting() error = %v, want ErrInvalidPosting", err)
			}
		})
	}
}
