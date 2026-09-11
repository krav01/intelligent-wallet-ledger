package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	riskdomain "github.com/krav01/intelligent-wallet-ledger/internal/risk/domain"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
)

func TestNewRepositoryRejectsNilPool(t *testing.T) {
	t.Parallel()

	_, err := NewRepository(nil)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("NewRepository(nil) error = %v, want ErrInvalidArgument", err)
	}
}

func TestPrepareTransfer(t *testing.T) {
	t.Parallel()

	transfer := mustTransfer(t, transferdomain.NewTransferParams{
		ID:                   "11111111-1111-4111-8111-111111111111",
		IdempotencyKey:       "request-1",
		RequesterID:          "22222222-2222-4222-8222-222222222222",
		SourceAccountID:      "33333333-3333-4333-8333-333333333333",
		DestinationAccountID: "44444444-4444-4444-8444-444444444444",
		Amount:               mustMoney(t, 50, "USD"),
		RequestedAt:          time.Date(2026, time.September, 10, 10, 0, 0, 0, time.UTC),
	})

	prepared, err := prepareTransfer(transfer)
	if err != nil {
		t.Fatalf("prepareTransfer() error = %v", err)
	}
	if prepared.id.String() != transfer.ID() || prepared.entry.ID() != transfer.ID() {
		t.Errorf("prepareTransfer() IDs differ from %q", transfer.ID())
	}
}

func TestPrepareTransferRejectsZeroValue(t *testing.T) {
	t.Parallel()

	_, err := prepareTransfer(transferdomain.Transfer{})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("prepareTransfer() error = %v, want ErrInvalidArgument", err)
	}
}

func TestSameIntentIgnoresServerIdentity(t *testing.T) {
	t.Parallel()

	base := transferdomain.NewTransferParams{
		ID:                   "11111111-1111-4111-8111-111111111111",
		IdempotencyKey:       "request-1",
		RequesterID:          "22222222-2222-4222-8222-222222222222",
		SourceAccountID:      "33333333-3333-4333-8333-333333333333",
		DestinationAccountID: "44444444-4444-4444-8444-444444444444",
		Amount:               mustMoney(t, 50, "USD"),
		RequestedAt:          time.Date(2026, time.September, 10, 10, 0, 0, 0, time.UTC),
	}
	left := mustTransfer(t, base)
	base.ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	base.RequestedAt = base.RequestedAt.Add(time.Hour)
	right := mustTransfer(t, base)
	if !sameIntent(left, right) {
		t.Fatal("sameIntent() = false for matching client intent")
	}

	base.Amount = mustMoney(t, 51, "USD")
	if sameIntent(left, mustTransfer(t, base)) {
		t.Fatal("sameIntent() = true for different amount")
	}
}

func TestClassifyCreateError(t *testing.T) {
	t.Parallel()

	err := classifyCreateError(&pgconn.PgError{Code: "23505", ConstraintName: "transfers_pkey"})
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("classifyCreateError() = %v, want ErrAlreadyExists", err)
	}
}

func TestPrepareRiskAssessment(t *testing.T) {
	t.Parallel()

	pending, err := transferdomain.NewPendingLifecycle(mustTransfer(t, transferdomain.NewTransferParams{
		ID:                   "11111111-1111-4111-8111-111111111111",
		IdempotencyKey:       "risk-assessment",
		RequesterID:          "22222222-2222-4222-8222-222222222222",
		SourceAccountID:      "33333333-3333-4333-8333-333333333333",
		DestinationAccountID: "44444444-4444-4444-8444-444444444444",
		Amount:               mustMoney(t, 60, "USD"),
		RequestedAt:          time.Date(2026, time.September, 12, 10, 0, 0, 0, time.UTC),
	}), "risk-v1")
	if err != nil {
		t.Fatalf("NewPendingLifecycle() error = %v", err)
	}
	policy, err := riskdomain.NewPolicy(riskdomain.PolicyParams{
		Version: "risk-v1",
		Thresholds: []riskdomain.Threshold{{
			Currency:           pending.Transfer().Amount().Currency(),
			ReviewAmountMinor:  50,
			DeclineAmountMinor: 100,
		}},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	evaluation, err := policy.Evaluate(riskdomain.Input{Amount: pending.Transfer().Amount()})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	next, err := pending.RequireReview()
	if err != nil {
		t.Fatalf("RequireReview() error = %v", err)
	}

	input, signals, causationID, err := prepareRiskAssessment(
		pending,
		next,
		evaluation,
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		pending.Transfer().RequestedAt(),
	)
	if err != nil {
		t.Fatalf("prepareRiskAssessment() error = %v", err)
	}
	if string(input) != `{"amount_minor":60,"currency":"USD"}` ||
		string(signals) != `[{"code":"amount_review_threshold","contribution":600}]` ||
		causationID.String() != "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" {
		t.Errorf("prepareRiskAssessment() = (%s, %s, %s), want canonical assessment", input, signals, causationID.String())
	}

	_, _, _, err = prepareRiskAssessment(
		pending,
		next,
		evaluation,
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		time.Time{},
	)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("prepareRiskAssessment(zero time) error = %v, want ErrInvalidArgument", err)
	}
}

func mustTransfer(t testing.TB, params transferdomain.NewTransferParams) transferdomain.Transfer {
	t.Helper()

	transfer, err := transferdomain.NewTransfer(params)
	if err != nil {
		t.Fatalf("NewTransfer() error = %v", err)
	}

	return transfer
}

func mustMoney(t testing.TB, minorUnits int64, currencyCode string) ledgerdomain.Money {
	t.Helper()

	currency, err := ledgerdomain.ParseCurrency(currencyCode)
	if err != nil {
		t.Fatalf("ParseCurrency() error = %v", err)
	}
	money, err := ledgerdomain.NewMoney(minorUnits, currency)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}

	return money
}
