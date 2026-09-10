package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/krav01/intelligent-wallet-ledger/internal/event"
)

func TestNewRepositoryRejectsNilPool(t *testing.T) {
	t.Parallel()
	if _, err := NewRepository(nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("NewRepository(nil) error = %v, want ErrInvalidArgument", err)
	}
}

func TestAddTxRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	if _, err := AddTx(context.Background(), nil, event.Draft{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("AddTx(nil) error = %v, want ErrInvalidArgument", err)
	}
}

func TestClaimRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	repository := &Repository{}
	tests := []struct {
		name  string
		limit int
		lease time.Duration
	}{
		{name: "zero limit", limit: 0, lease: time.Second},
		{name: "large limit", limit: 101, lease: time.Second},
		{name: "short lease", limit: 1, lease: time.Millisecond},
		{name: "fractional lease", limit: 1, lease: time.Second + time.Millisecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := repository.Claim(context.Background(), test.limit, test.lease)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("Claim() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestSanitizeDiagnostic(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{input: "", want: fallbackDiagnostic},
		{input: "secret\nnext", want: "secret?next"},
		{input: "broker ОК", want: "broker ??"},
		{input: strings.Repeat("x", 1025), want: strings.Repeat("x", 1024)},
	}
	for _, test := range tests {
		if got := sanitizeDiagnostic(test.input); got != test.want {
			t.Errorf("sanitizeDiagnostic(%q) = %q, want %q", test.input, got, test.want)
		}
	}
}

func TestNewClaimTokenCreatesDistinctUUIDs(t *testing.T) {
	t.Parallel()
	first, err := newClaimToken()
	if err != nil {
		t.Fatalf("newClaimToken() error = %v", err)
	}
	second, err := newClaimToken()
	if err != nil {
		t.Fatalf("newClaimToken() second error = %v", err)
	}
	if first == second {
		t.Fatalf("newClaimToken() returned duplicate token %q", first)
	}
	if _, err := parseUUID(first); err != nil {
		t.Fatalf("newClaimToken() token %q is invalid: %v", first, err)
	}
}

func TestClassifyWriteErrorMapsPostgreSQLDataRejection(t *testing.T) {
	t.Parallel()
	err := classifyWriteError(&pgconn.PgError{Code: "22P05", Message: "unsupported Unicode escape"})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("classifyWriteError() error = %v, want ErrInvalidArgument", err)
	}
}
