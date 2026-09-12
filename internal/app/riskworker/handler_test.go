package riskworker

import (
	"errors"
	"testing"
	"time"

	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	riskdomain "github.com/krav01/intelligent-wallet-ledger/internal/risk/domain"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
)

func TestTransition(t *testing.T) {
	pending := mustPendingLifecycle(t)

	tests := []struct {
		name     string
		decision riskdomain.Decision
		want     transferdomain.Status
	}{
		{name: "approve", decision: riskdomain.DecisionApprove, want: transferdomain.StatusApproved},
		{name: "review", decision: riskdomain.DecisionReview, want: transferdomain.StatusReviewRequired},
		{name: "decline", decision: riskdomain.DecisionDecline, want: transferdomain.StatusDeclined},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			next, err := transition(pending, test.decision)
			if err != nil {
				t.Fatalf("transition() error = %v", err)
			}
			if next.Status() != test.want || next.Version() != 2 {
				t.Errorf("transition() = (%s, %d), want (%s, 2)", next.Status(), next.Version(), test.want)
			}
		})
	}
	if _, err := transition(pending, riskdomain.DecisionUnknown); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("transition(unknown) error = %v", err)
	}
}

func mustPendingLifecycle(t *testing.T) transferdomain.Lifecycle {
	t.Helper()
	currency, err := ledgerdomain.ParseCurrency("USD")
	if err != nil {
		t.Fatal(err)
	}
	money, err := ledgerdomain.NewMoney(1, currency)
	if err != nil {
		t.Fatal(err)
	}
	transfer, err := transferdomain.NewTransfer(transferdomain.NewTransferParams{ID: "11111111-1111-4111-8111-111111111111", IdempotencyKey: "request", RequesterID: "22222222-2222-4222-8222-222222222222", SourceAccountID: "33333333-3333-4333-8333-333333333333", DestinationAccountID: "44444444-4444-4444-8444-444444444444", Amount: money, RequestedAt: time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	lifecycle, err := transferdomain.NewPendingLifecycle(transfer, "risk-v1")
	if err != nil {
		t.Fatal(err)
	}
	return lifecycle
}
