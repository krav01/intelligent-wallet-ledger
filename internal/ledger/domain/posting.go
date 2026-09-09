package domain

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

// ErrInvalidPosting indicates that a posting violates a ledger invariant.
var ErrInvalidPosting = errors.New("ledger domain: invalid posting")

// Posting is an immutable signed movement on one account.
// Positive amounts increase an account balance; negative amounts decrease it.
type Posting struct {
	accountID string
	amount    Money
}

// NewPosting creates a non-zero movement for an account.
func NewPosting(accountID string, amount Money) (Posting, error) {
	posting := Posting{
		accountID: strings.TrimSpace(accountID),
		amount:    amount,
	}
	if err := posting.validate(); err != nil {
		return Posting{}, err
	}

	return posting, nil
}

// AccountID returns the account affected by the posting.
func (p Posting) AccountID() string {
	return p.accountID
}

// Amount returns the signed movement in minor currency units.
func (p Posting) Amount() Money {
	return p.amount
}

func (p Posting) validate() error {
	if p.accountID == "" {
		return fmt.Errorf("%w: account ID is required", ErrInvalidPosting)
	}
	if !p.amount.currency.valid() {
		return fmt.Errorf("%w: amount: %w", ErrInvalidPosting, ErrInvalidCurrency)
	}
	if p.amount.minorUnits == 0 {
		return fmt.Errorf("%w: amount must be non-zero", ErrInvalidPosting)
	}
	if p.amount.minorUnits == math.MinInt64 {
		return fmt.Errorf(
			"%w: amount must be reversible: %w",
			ErrInvalidPosting,
			ErrAmountOverflow,
		)
	}

	return nil
}
