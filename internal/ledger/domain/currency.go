// Package domain contains the financial invariants owned by the ledger.
package domain

import (
	"errors"
	"fmt"
)

// ErrInvalidCurrency indicates that a currency is missing or malformed.
var ErrInvalidCurrency = errors.New("ledger domain: invalid currency")

// Currency is an immutable three-letter uppercase currency code.
//
// The type validates the shape of a code rather than maintaining an embedded
// ISO 4217 registry. Supported currencies are a product policy concern and can
// be restricted at application boundaries without making this package stale.
type Currency struct {
	code [3]byte
}

// ParseCurrency creates a Currency from a strict uppercase ASCII code.
func ParseCurrency(code string) (Currency, error) {
	if len(code) != len(Currency{}.code) {
		return Currency{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
	}

	var parsed Currency
	for index := range parsed.code {
		character := code[index]
		if character < 'A' || character > 'Z' {
			return Currency{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
		}

		parsed.code[index] = character
	}

	return parsed, nil
}

// String returns the currency code. An invalid zero value returns an empty string.
func (c Currency) String() string {
	if !c.valid() {
		return ""
	}

	return string(c.code[:])
}

func (c Currency) valid() bool {
	for _, character := range c.code {
		if character < 'A' || character > 'Z' {
			return false
		}
	}

	return true
}
