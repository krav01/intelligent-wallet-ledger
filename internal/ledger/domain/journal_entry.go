package domain

import (
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"time"
)

const (
	minPostingsPerEntry = 2
	maxPostingsPerEntry = 1024
)

var (
	// ErrInvalidJournalEntry indicates malformed entry identity, time, or postings.
	ErrInvalidJournalEntry = errors.New("ledger domain: invalid journal entry")
	// ErrUnbalancedJournalEntry indicates that postings do not sum to zero per currency.
	ErrUnbalancedJournalEntry = errors.New("ledger domain: unbalanced journal entry")
)

// JournalEntryType distinguishes original financial entries from corrections.
type JournalEntryType uint8

const (
	// JournalEntryTypeUnknown is the invalid zero value.
	JournalEntryTypeUnknown JournalEntryType = iota
	// JournalEntryTypeStandard is an original financial entry.
	JournalEntryTypeStandard
	// JournalEntryTypeReversal corrects an earlier entry without mutating it.
	JournalEntryTypeReversal
)

// NewJournalEntryParams contains the identity, immutable postings, and recording time.
type NewJournalEntryParams struct {
	ID         string
	Postings   []Posting
	RecordedAt time.Time
}

// JournalEntry is an immutable group of postings that balances per currency.
type JournalEntry struct {
	id              string
	entryType       JournalEntryType
	reversesEntryID string
	postings        []Posting
	recordedAt      time.Time
}

// NewJournalEntry creates a balanced original financial entry.
func NewJournalEntry(params NewJournalEntryParams) (JournalEntry, error) {
	return newJournalEntry(
		params,
		JournalEntryTypeStandard,
		"",
	)
}

// ID returns the immutable journal entry identifier.
func (e JournalEntry) ID() string {
	return e.id
}

// Type returns whether the entry is original or a reversal.
func (e JournalEntry) Type() JournalEntryType {
	return e.entryType
}

// ReversesEntryID returns the corrected entry ID, or an empty string for standard entries.
func (e JournalEntry) ReversesEntryID() string {
	return e.reversesEntryID
}

// Postings returns a defensive copy of the entry's postings.
func (e JournalEntry) Postings() []Posting {
	return slices.Clone(e.postings)
}

// RecordedAt returns when the entry was recorded, normalized to UTC.
func (e JournalEntry) RecordedAt() time.Time {
	return e.recordedAt
}

// Reverse creates a new entry with opposite postings and a link to this entry.
func (e JournalEntry) Reverse(newID string, recordedAt time.Time) (JournalEntry, error) {
	if !e.valid() {
		return JournalEntry{}, fmt.Errorf(
			"reversing journal entry: %w: source entry is invalid",
			ErrInvalidJournalEntry,
		)
	}
	if strings.TrimSpace(newID) == e.id {
		return JournalEntry{}, fmt.Errorf(
			"reversing journal entry: %w: reversal ID must differ from source ID",
			ErrInvalidJournalEntry,
		)
	}

	reversedPostings := make([]Posting, 0, len(e.postings))
	for _, posting := range e.postings {
		amount, err := posting.amount.Negate()
		if err != nil {
			return JournalEntry{}, fmt.Errorf("reversing journal entry: posting amount: %w", err)
		}

		reversedPosting, err := NewPosting(posting.accountID, amount)
		if err != nil {
			return JournalEntry{}, fmt.Errorf("reversing journal entry: %w", err)
		}
		reversedPostings = append(reversedPostings, reversedPosting)
	}

	return newJournalEntry(
		NewJournalEntryParams{
			ID:         newID,
			Postings:   reversedPostings,
			RecordedAt: recordedAt,
		},
		JournalEntryTypeReversal,
		e.id,
	)
}

func newJournalEntry(
	params NewJournalEntryParams,
	entryType JournalEntryType,
	reversesEntryID string,
) (JournalEntry, error) {
	entry := JournalEntry{
		id:              strings.TrimSpace(params.ID),
		entryType:       entryType,
		reversesEntryID: reversesEntryID,
		postings:        slices.Clone(params.Postings),
		recordedAt:      params.RecordedAt.UTC(),
	}
	if err := entry.validate(); err != nil {
		return JournalEntry{}, err
	}

	return entry, nil
}

func (e JournalEntry) valid() bool {
	return e.validate() == nil
}

func (e JournalEntry) validate() error {
	if e.id == "" {
		return fmt.Errorf("%w: ID is required", ErrInvalidJournalEntry)
	}
	if e.recordedAt.IsZero() {
		return fmt.Errorf("%w: recorded time is required", ErrInvalidJournalEntry)
	}
	if e.entryType != JournalEntryTypeStandard && e.entryType != JournalEntryTypeReversal {
		return fmt.Errorf("%w: type is invalid", ErrInvalidJournalEntry)
	}
	if e.entryType == JournalEntryTypeStandard && e.reversesEntryID != "" {
		return fmt.Errorf("%w: standard entry cannot reverse another entry", ErrInvalidJournalEntry)
	}
	if e.entryType == JournalEntryTypeReversal && e.reversesEntryID == "" {
		return fmt.Errorf("%w: reversal source ID is required", ErrInvalidJournalEntry)
	}
	if len(e.postings) < minPostingsPerEntry || len(e.postings) > maxPostingsPerEntry {
		return fmt.Errorf(
			"%w: posting count must be between %d and %d",
			ErrInvalidJournalEntry,
			minPostingsPerEntry,
			maxPostingsPerEntry,
		)
	}

	for index, posting := range e.postings {
		if err := posting.validate(); err != nil {
			return fmt.Errorf("%w: posting %d: %w", ErrInvalidJournalEntry, index, err)
		}
	}
	if err := validateBalancedPostings(e.postings); err != nil {
		return err
	}

	return nil
}

func validateBalancedPostings(postings []Posting) error {
	totals := make(map[Currency]*big.Int)
	for _, posting := range postings {
		total, exists := totals[posting.amount.currency]
		if !exists {
			total = new(big.Int)
			totals[posting.amount.currency] = total
		}
		total.Add(total, big.NewInt(posting.amount.minorUnits))
	}

	for currency, total := range totals {
		if total.Sign() != 0 {
			return fmt.Errorf(
				"%w: currency %s has minor-unit total %s",
				ErrUnbalancedJournalEntry,
				currency.String(),
				total.String(),
			)
		}
	}

	return nil
}
