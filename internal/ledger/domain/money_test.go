package domain_test

import (
	"errors"
	"math"
	"testing"

	"github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
)

func TestNewMoney(t *testing.T) {
	t.Parallel()

	currency := mustCurrency(t, "USD")
	amount, err := domain.NewMoney(-125, currency)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	if got := amount.MinorUnits(); got != -125 {
		t.Errorf("Money.MinorUnits() = %d, want -125", got)
	}
	if got := amount.Currency().String(); got != "USD" {
		t.Errorf("Money.Currency() = %q, want USD", got)
	}
}

func TestNewMoneyRejectsInvalidCurrency(t *testing.T) {
	t.Parallel()

	_, err := domain.NewMoney(100, domain.Currency{})
	if !errors.Is(err, domain.ErrInvalidCurrency) {
		t.Fatalf("NewMoney() error = %v, want ErrInvalidCurrency", err)
	}
}

func TestMoneyAdd(t *testing.T) {
	t.Parallel()

	usd := mustCurrency(t, "USD")
	tests := []struct {
		name  string
		left  int64
		right int64
		want  int64
	}{
		{name: "positive amounts", left: 125, right: 75, want: 200},
		{name: "credit and debit", left: 125, right: -75, want: 50},
		{name: "zero", left: 125, right: 0, want: 125},
		{name: "minimum boundary", left: math.MinInt64 + 1, right: -1, want: math.MinInt64},
		{name: "maximum boundary", left: math.MaxInt64 - 1, right: 1, want: math.MaxInt64},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			left := mustMoney(t, test.left, usd)
			right := mustMoney(t, test.right, usd)
			got, err := left.Add(right)
			if err != nil {
				t.Fatalf("Money.Add() error = %v", err)
			}
			if got.MinorUnits() != test.want {
				t.Errorf("Money.Add().MinorUnits() = %d, want %d", got.MinorUnits(), test.want)
			}
		})
	}
}

func TestMoneyAddRejectsInvalidOperation(t *testing.T) {
	t.Parallel()

	usd := mustCurrency(t, "USD")
	eur := mustCurrency(t, "EUR")
	tests := []struct {
		name  string
		left  domain.Money
		right domain.Money
		want  error
	}{
		{
			name:  "currency mismatch",
			left:  mustMoney(t, 100, usd),
			right: mustMoney(t, 100, eur),
			want:  domain.ErrCurrencyMismatch,
		},
		{
			name:  "positive overflow",
			left:  mustMoney(t, math.MaxInt64, usd),
			right: mustMoney(t, 1, usd),
			want:  domain.ErrAmountOverflow,
		},
		{
			name:  "negative overflow",
			left:  mustMoney(t, math.MinInt64, usd),
			right: mustMoney(t, -1, usd),
			want:  domain.ErrAmountOverflow,
		},
		{
			name:  "invalid zero value",
			left:  domain.Money{},
			right: mustMoney(t, 1, usd),
			want:  domain.ErrInvalidCurrency,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := test.left.Add(test.right)
			if !errors.Is(err, test.want) {
				t.Fatalf("Money.Add() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestMoneySubtract(t *testing.T) {
	t.Parallel()

	usd := mustCurrency(t, "USD")
	left := mustMoney(t, 125, usd)
	right := mustMoney(t, 75, usd)

	got, err := left.Subtract(right)
	if err != nil {
		t.Fatalf("Money.Subtract() error = %v", err)
	}
	if got.MinorUnits() != 50 {
		t.Errorf("Money.Subtract().MinorUnits() = %d, want 50", got.MinorUnits())
	}
}

func TestMoneySubtractRejectsInvalidOperation(t *testing.T) {
	t.Parallel()

	usd := mustCurrency(t, "USD")
	eur := mustCurrency(t, "EUR")
	tests := []struct {
		name  string
		left  domain.Money
		right domain.Money
		want  error
	}{
		{
			name:  "currency mismatch",
			left:  mustMoney(t, 100, usd),
			right: mustMoney(t, 100, eur),
			want:  domain.ErrCurrencyMismatch,
		},
		{
			name:  "negative overflow",
			left:  mustMoney(t, math.MinInt64, usd),
			right: mustMoney(t, 1, usd),
			want:  domain.ErrAmountOverflow,
		},
		{
			name:  "positive overflow",
			left:  mustMoney(t, math.MaxInt64, usd),
			right: mustMoney(t, -1, usd),
			want:  domain.ErrAmountOverflow,
		},
		{
			name:  "invalid zero value",
			left:  domain.Money{},
			right: mustMoney(t, 1, usd),
			want:  domain.ErrInvalidCurrency,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := test.left.Subtract(test.right)
			if !errors.Is(err, test.want) {
				t.Fatalf("Money.Subtract() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestMoneyNegate(t *testing.T) {
	t.Parallel()

	usd := mustCurrency(t, "USD")
	tests := []struct {
		name  string
		value int64
		want  int64
	}{
		{name: "positive", value: 125, want: -125},
		{name: "negative", value: -125, want: 125},
		{name: "zero", value: 0, want: 0},
		{name: "maximum", value: math.MaxInt64, want: -math.MaxInt64},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			amount := mustMoney(t, test.value, usd)
			got, err := amount.Negate()
			if err != nil {
				t.Fatalf("Money.Negate() error = %v", err)
			}
			if got.MinorUnits() != test.want {
				t.Errorf("Money.Negate().MinorUnits() = %d, want %d", got.MinorUnits(), test.want)
			}
		})
	}
}

func TestMoneyNegateRejectsInvalidOperation(t *testing.T) {
	t.Parallel()

	usd := mustCurrency(t, "USD")
	tests := []struct {
		name  string
		value domain.Money
		want  error
	}{
		{
			name:  "minimum integer",
			value: mustMoney(t, math.MinInt64, usd),
			want:  domain.ErrAmountOverflow,
		},
		{
			name:  "invalid zero value",
			value: domain.Money{},
			want:  domain.ErrInvalidCurrency,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := test.value.Negate()
			if !errors.Is(err, test.want) {
				t.Fatalf("Money.Negate() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestMoneyArithmeticProperties(t *testing.T) {
	t.Parallel()

	usd := mustCurrency(t, "USD")
	values := []int64{
		math.MinInt64,
		math.MinInt64 + 1,
		-1,
		0,
		1,
		math.MaxInt64 - 1,
		math.MaxInt64,
	}

	for _, leftUnits := range values {
		for _, rightUnits := range values {
			left := mustMoney(t, leftUnits, usd)
			right := mustMoney(t, rightUnits, usd)

			leftRight, leftRightErr := left.Add(right)
			rightLeft, rightLeftErr := right.Add(left)
			if !sameError(leftRightErr, rightLeftErr) {
				t.Fatalf(
					"Add(%d, %d) error = %v, reverse error = %v",
					leftUnits,
					rightUnits,
					leftRightErr,
					rightLeftErr,
				)
			}
			if errors.Is(leftRightErr, domain.ErrAmountOverflow) {
				continue
			}
			if leftRightErr != nil {
				t.Fatalf("Add(%d, %d) error = %v", leftUnits, rightUnits, leftRightErr)
			}
			if leftRight.MinorUnits() != rightLeft.MinorUnits() {
				t.Errorf(
					"Add(%d, %d) = %d, reverse = %d",
					leftUnits,
					rightUnits,
					leftRight.MinorUnits(),
					rightLeft.MinorUnits(),
				)
			}

			restored, err := leftRight.Subtract(right)
			if err != nil {
				t.Fatalf("Subtracting %d from sum error = %v", rightUnits, err)
			}
			if restored.MinorUnits() != leftUnits {
				t.Errorf("(left + right) - right = %d, want %d", restored.MinorUnits(), leftUnits)
			}
		}
	}
}

func FuzzMoneyAddSubtractRoundTrip(f *testing.F) {
	f.Add(int64(0), int64(0))
	f.Add(int64(125), int64(-75))
	f.Add(int64(math.MaxInt64), int64(1))
	f.Add(int64(math.MinInt64), int64(-1))

	usd, err := domain.ParseCurrency("USD")
	if err != nil {
		f.Fatalf("ParseCurrency(USD) error = %v", err)
	}

	f.Fuzz(func(t *testing.T, leftUnits, rightUnits int64) {
		left := mustMoney(t, leftUnits, usd)
		right := mustMoney(t, rightUnits, usd)

		sum, err := left.Add(right)
		if errors.Is(err, domain.ErrAmountOverflow) {
			return
		}
		if err != nil {
			t.Fatalf("Money.Add() error = %v", err)
		}

		restored, err := sum.Subtract(right)
		if err != nil {
			t.Fatalf("Money.Subtract() error = %v", err)
		}
		if restored.MinorUnits() != leftUnits {
			t.Errorf("(left + right) - right = %d, want %d", restored.MinorUnits(), leftUnits)
		}
	})
}

func mustCurrency(t testing.TB, code string) domain.Currency {
	t.Helper()

	currency, err := domain.ParseCurrency(code)
	if err != nil {
		t.Fatalf("ParseCurrency(%q) error = %v", code, err)
	}

	return currency
}

func mustMoney(t testing.TB, minorUnits int64, currency domain.Currency) domain.Money {
	t.Helper()

	amount, err := domain.NewMoney(minorUnits, currency)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}

	return amount
}

func sameError(left, right error) bool {
	switch {
	case left == nil && right == nil:
		return true
	case errors.Is(left, domain.ErrAmountOverflow) && errors.Is(right, domain.ErrAmountOverflow):
		return true
	default:
		return false
	}
}
