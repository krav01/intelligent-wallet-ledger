package domain

import (
	"errors"
	"fmt"
	"math"
)

var (
	// ErrCurrencyMismatch indicates an operation attempted to combine different currencies.
	ErrCurrencyMismatch = errors.New("ledger domain: currency mismatch")
	// ErrAmountOverflow indicates that a money operation exceeded the int64 minor-unit range.
	ErrAmountOverflow = errors.New("ledger domain: amount overflow")
)

// Money is an immutable signed amount expressed in minor currency units.
// Negative values are valid because ledger postings have debit and credit sides.
type Money struct {
	minorUnits int64
	currency   Currency
}

// NewMoney creates a Money value without floating-point conversion.
func NewMoney(minorUnits int64, currency Currency) (Money, error) {
	if !currency.valid() {
		return Money{}, ErrInvalidCurrency
	}

	return Money{
		minorUnits: minorUnits,
		currency:   currency,
	}, nil
}

// MinorUnits returns the signed amount in the currency's smallest unit.
func (m Money) MinorUnits() int64 {
	return m.minorUnits
}

// Currency returns the amount's currency.
func (m Money) Currency() Currency {
	return m.currency
}

// Add returns the sum of two amounts with the same currency.
func (m Money) Add(other Money) (Money, error) {
	if err := m.validateOperation(other); err != nil {
		return Money{}, fmt.Errorf("adding money: %w", err)
	}
	if additionOverflows(m.minorUnits, other.minorUnits) {
		return Money{}, fmt.Errorf("adding money: %w", ErrAmountOverflow)
	}

	return NewMoney(m.minorUnits+other.minorUnits, m.currency)
}

// Subtract returns the difference between two amounts with the same currency.
func (m Money) Subtract(other Money) (Money, error) {
	if err := m.validateOperation(other); err != nil {
		return Money{}, fmt.Errorf("subtracting money: %w", err)
	}
	if subtractionOverflows(m.minorUnits, other.minorUnits) {
		return Money{}, fmt.Errorf("subtracting money: %w", ErrAmountOverflow)
	}

	return NewMoney(m.minorUnits-other.minorUnits, m.currency)
}

// Negate returns an amount with the opposite sign.
func (m Money) Negate() (Money, error) {
	if !m.currency.valid() {
		return Money{}, fmt.Errorf("negating money: %w", ErrInvalidCurrency)
	}
	if m.minorUnits == math.MinInt64 {
		return Money{}, fmt.Errorf("negating money: %w", ErrAmountOverflow)
	}

	return NewMoney(-m.minorUnits, m.currency)
}

func (m Money) validateOperation(other Money) error {
	if !m.currency.valid() || !other.currency.valid() {
		return ErrInvalidCurrency
	}
	if m.currency != other.currency {
		return ErrCurrencyMismatch
	}

	return nil
}

func additionOverflows(left, right int64) bool {
	return right > 0 && left > math.MaxInt64-right ||
		right < 0 && left < math.MinInt64-right
}

func subtractionOverflows(left, right int64) bool {
	return right > 0 && left < math.MinInt64+right ||
		right < 0 && left > math.MaxInt64+right
}
