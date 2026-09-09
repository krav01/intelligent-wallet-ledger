// Package wallet contains wallet and account domain read models.
package wallet

import (
	"errors"
	"fmt"
	"time"

	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
)

// ErrInvalidStatus indicates a wallet or account status outside the domain lifecycle.
var ErrInvalidStatus = errors.New("wallet domain: invalid status")

// Status is the lifecycle state shared by wallets and accounts.
type Status string

const (
	// StatusActive permits normal wallet or account operations.
	StatusActive Status = "active"
	// StatusFrozen temporarily blocks wallet or account operations.
	StatusFrozen Status = "frozen"
	// StatusClosed permanently ends the wallet or account lifecycle.
	StatusClosed Status = "closed"
)

// ParseStatus validates a persisted lifecycle state.
func ParseStatus(value string) (Status, error) {
	status := Status(value)
	switch status {
	case StatusActive, StatusFrozen, StatusClosed:
		return status, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrInvalidStatus, value)
	}
}

// Wallet is a persisted wallet with its currency accounts.
type Wallet struct {
	ID        string
	OwnerID   string
	Status    Status
	Accounts  []Account
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Account is a currency-specific account and its transactional balance snapshot.
type Account struct {
	ID             string
	WalletID       string
	Status         Status
	Balance        ledgerdomain.Money
	BalanceVersion int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
