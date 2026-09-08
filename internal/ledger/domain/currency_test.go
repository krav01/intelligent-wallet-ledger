package domain_test

import (
	"errors"
	"testing"

	"github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
)

func TestParseCurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code string
		want string
	}{
		{name: "US dollar", code: "USD", want: "USD"},
		{name: "euro", code: "EUR", want: "EUR"},
		{name: "syntactically valid private code", code: "XTS", want: "XTS"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			currency, err := domain.ParseCurrency(test.code)
			if err != nil {
				t.Fatalf("ParseCurrency(%q) error = %v", test.code, err)
			}
			if got := currency.String(); got != test.want {
				t.Errorf("Currency.String() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestParseCurrencyRejectsInvalidCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code string
	}{
		{name: "empty", code: ""},
		{name: "too short", code: "US"},
		{name: "too long", code: "USDT"},
		{name: "lowercase", code: "usd"},
		{name: "digit", code: "US1"},
		{name: "non ASCII", code: "EU€"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			currency, err := domain.ParseCurrency(test.code)
			if !errors.Is(err, domain.ErrInvalidCurrency) {
				t.Fatalf("ParseCurrency(%q) error = %v, want ErrInvalidCurrency", test.code, err)
			}
			if got := currency.String(); got != "" {
				t.Errorf("invalid Currency.String() = %q, want empty string", got)
			}
		})
	}
}

func TestCurrencyZeroValueIsInvalid(t *testing.T) {
	t.Parallel()

	var currency domain.Currency
	if got := currency.String(); got != "" {
		t.Errorf("zero-value Currency.String() = %q, want empty string", got)
	}
}
