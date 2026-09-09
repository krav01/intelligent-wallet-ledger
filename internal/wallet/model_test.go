package wallet_test

import (
	"errors"
	"testing"

	"github.com/krav01/intelligent-wallet-ledger/internal/wallet"
)

func TestParseStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  wallet.Status
	}{
		{name: "active", value: "active", want: wallet.StatusActive},
		{name: "frozen", value: "frozen", want: wallet.StatusFrozen},
		{name: "closed", value: "closed", want: wallet.StatusClosed},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := wallet.ParseStatus(test.value)
			if err != nil {
				t.Fatalf("ParseStatus(%q) error = %v", test.value, err)
			}
			if got != test.want {
				t.Errorf("ParseStatus(%q) = %q, want %q", test.value, got, test.want)
			}
		})
	}
}

func TestParseStatusRejectsInvalidValue(t *testing.T) {
	t.Parallel()

	_, err := wallet.ParseStatus("pending")
	if !errors.Is(err, wallet.ErrInvalidStatus) {
		t.Fatalf("ParseStatus(pending) error = %v, want ErrInvalidStatus", err)
	}
}
