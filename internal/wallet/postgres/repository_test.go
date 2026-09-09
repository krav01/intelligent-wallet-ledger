package postgres

import (
	"errors"
	"reflect"
	"testing"

	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
)

func TestNewRepositoryRejectsNilPool(t *testing.T) {
	t.Parallel()

	_, err := NewRepository(nil)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("NewRepository(nil) error = %v, want ErrInvalidArgument", err)
	}
}

func TestNormalizeCurrencies(t *testing.T) {
	t.Parallel()

	eur := mustCurrency(t, "EUR")
	usd := mustCurrency(t, "USD")

	got, err := normalizeCurrencies([]ledgerdomain.Currency{usd, eur})
	if err != nil {
		t.Fatalf("normalizeCurrencies() error = %v", err)
	}
	want := []string{"EUR", "USD"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("normalizeCurrencies() = %v, want %v", got, want)
	}
}

func TestNormalizeCurrenciesRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	usd := mustCurrency(t, "USD")
	tooMany := make([]ledgerdomain.Currency, 0, maxCurrenciesPerWallet+1)
	for index := range maxCurrenciesPerWallet + 1 {
		code := string([]byte{'A', byte('A' + index/26), byte('A' + index%26)})
		tooMany = append(tooMany, mustCurrency(t, code))
	}
	tests := []struct {
		name       string
		currencies []ledgerdomain.Currency
	}{
		{name: "empty", currencies: []ledgerdomain.Currency{}},
		{name: "invalid zero value", currencies: []ledgerdomain.Currency{{}}},
		{name: "duplicate", currencies: []ledgerdomain.Currency{usd, usd}},
		{name: "too many", currencies: tooMany},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := normalizeCurrencies(test.currencies)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("normalizeCurrencies() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestParseUUIDRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "malformed", value: "not-a-uuid"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := parseUUID(test.value); err == nil {
				t.Fatalf("parseUUID(%q) error = nil, want error", test.value)
			}
		})
	}
}

func mustCurrency(t testing.TB, code string) ledgerdomain.Currency {
	t.Helper()

	currency, err := ledgerdomain.ParseCurrency(code)
	if err != nil {
		t.Fatalf("ParseCurrency(%q) error = %v", code, err)
	}

	return currency
}
