// Package domain defines transfer invariants independently of transport and storage.
package domain

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
)

const maxIdempotencyKeyBytes = 128

// ErrInvalidTransfer indicates malformed transfer identity, intent, amount, or time.
var ErrInvalidTransfer = errors.New("transfer domain: invalid transfer")

// NewTransferParams contains immutable transfer identity and money-movement intent.
type NewTransferParams struct {
	ID                   string
	IdempotencyKey       string
	RequesterID          string
	SourceAccountID      string
	DestinationAccountID string
	Amount               ledgerdomain.Money
	RequestedAt          time.Time
}

// Transfer is an immutable request to move a positive amount between two accounts.
type Transfer struct {
	id                   string
	idempotencyKey       string
	requesterID          string
	sourceAccountID      string
	destinationAccountID string
	amount               ledgerdomain.Money
	requestedAt          time.Time
}

// NewTransfer validates and creates a transfer.
func NewTransfer(params NewTransferParams) (Transfer, error) {
	id, _ := normalizeUUID(params.ID)
	requesterID, _ := normalizeUUID(params.RequesterID)
	sourceAccountID, _ := normalizeUUID(params.SourceAccountID)
	destinationAccountID, _ := normalizeUUID(params.DestinationAccountID)
	transfer := Transfer{
		id:                   id,
		idempotencyKey:       strings.TrimSpace(params.IdempotencyKey),
		requesterID:          requesterID,
		sourceAccountID:      sourceAccountID,
		destinationAccountID: destinationAccountID,
		amount:               params.Amount,
		requestedAt:          params.RequestedAt.UTC(),
	}
	if err := transfer.validate(); err != nil {
		return Transfer{}, err
	}

	return transfer, nil
}

// ID returns the server-assigned transfer identifier.
func (t Transfer) ID() string {
	return t.id
}

// IdempotencyKey returns the requester-scoped replay key.
func (t Transfer) IdempotencyKey() string {
	return t.idempotencyKey
}

// RequesterID returns the identity whose key namespace and source account are used.
func (t Transfer) RequesterID() string {
	return t.requesterID
}

// SourceAccountID returns the account that is debited.
func (t Transfer) SourceAccountID() string {
	return t.sourceAccountID
}

// DestinationAccountID returns the account that is credited.
func (t Transfer) DestinationAccountID() string {
	return t.destinationAccountID
}

// Amount returns the positive amount requested for transfer.
func (t Transfer) Amount() ledgerdomain.Money {
	return t.amount
}

// RequestedAt returns the request time normalized to UTC.
func (t Transfer) RequestedAt() time.Time {
	return t.requestedAt
}

// JournalEntry creates the balanced ledger entry for this transfer.
func (t Transfer) JournalEntry() (ledgerdomain.JournalEntry, error) {
	if err := t.validate(); err != nil {
		return ledgerdomain.JournalEntry{}, fmt.Errorf("creating transfer journal entry: %w", err)
	}

	debit, err := t.amount.Negate()
	if err != nil {
		return ledgerdomain.JournalEntry{}, fmt.Errorf("creating transfer debit: %w", err)
	}
	sourcePosting, err := ledgerdomain.NewPosting(t.sourceAccountID, debit)
	if err != nil {
		return ledgerdomain.JournalEntry{}, fmt.Errorf("creating transfer source posting: %w", err)
	}
	destinationPosting, err := ledgerdomain.NewPosting(t.destinationAccountID, t.amount)
	if err != nil {
		return ledgerdomain.JournalEntry{}, fmt.Errorf("creating transfer destination posting: %w", err)
	}

	entry, err := ledgerdomain.NewJournalEntry(ledgerdomain.NewJournalEntryParams{
		ID:         t.id,
		Postings:   []ledgerdomain.Posting{sourcePosting, destinationPosting},
		RecordedAt: t.requestedAt,
	})
	if err != nil {
		return ledgerdomain.JournalEntry{}, fmt.Errorf("creating transfer journal entry: %w", err)
	}

	return entry, nil
}

func (t Transfer) validate() error {
	switch {
	case !isCanonicalUUID(t.id):
		return fmt.Errorf("%w: ID must be a UUID", ErrInvalidTransfer)
	case !validIdempotencyKey(t.idempotencyKey):
		return fmt.Errorf(
			"%w: idempotency key must contain 1 to %d visible ASCII bytes",
			ErrInvalidTransfer,
			maxIdempotencyKeyBytes,
		)
	case !isCanonicalUUID(t.requesterID):
		return fmt.Errorf("%w: requester ID must be a UUID", ErrInvalidTransfer)
	case !isCanonicalUUID(t.sourceAccountID):
		return fmt.Errorf("%w: source account ID must be a UUID", ErrInvalidTransfer)
	case !isCanonicalUUID(t.destinationAccountID):
		return fmt.Errorf("%w: destination account ID must be a UUID", ErrInvalidTransfer)
	case t.sourceAccountID == t.destinationAccountID:
		return fmt.Errorf("%w: source and destination accounts must differ", ErrInvalidTransfer)
	case t.amount.Currency().String() == "":
		return fmt.Errorf("%w: amount currency is invalid", ErrInvalidTransfer)
	case t.amount.MinorUnits() <= 0:
		return fmt.Errorf("%w: amount must be positive", ErrInvalidTransfer)
	case t.requestedAt.IsZero():
		return fmt.Errorf("%w: requested time is required", ErrInvalidTransfer)
	default:
		return nil
	}
}

func normalizeUUID(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return "", false
	}
	hexValue := strings.ReplaceAll(value, "-", "")
	if len(hexValue) != 32 {
		return "", false
	}
	if _, err := hex.DecodeString(hexValue); err != nil {
		return "", false
	}

	return strings.ToLower(value), true
}

func isCanonicalUUID(value string) bool {
	canonical, valid := normalizeUUID(value)
	return valid && canonical == value
}

func validIdempotencyKey(value string) bool {
	if value == "" || len(value) > maxIdempotencyKeyBytes || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character < '!' || character > '~' {
			return false
		}
	}

	return true
}
