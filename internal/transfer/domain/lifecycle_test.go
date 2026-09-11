package domain_test

import (
	"errors"
	"testing"

	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
)

func TestNewPendingLifecycle(t *testing.T) {
	t.Parallel()
	transfer := mustTransfer(t)
	lifecycle, err := transferdomain.NewPendingLifecycle(transfer, "Risk-v1")
	if err != nil {
		t.Fatalf("NewPendingLifecycle() error = %v", err)
	}
	if lifecycle.Status() != transferdomain.StatusPendingRisk || lifecycle.Version() != 1 ||
		lifecycle.RiskPolicyVersion() != "Risk-v1" {
		t.Errorf(
			"NewPendingLifecycle() = (%s, %d, %q), want (pending_risk, 1, Risk-v1)",
			lifecycle.Status(),
			lifecycle.Version(),
			lifecycle.RiskPolicyVersion(),
		)
	}
}

func TestLifecycleTransitions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		transition func(transferdomain.Lifecycle) (transferdomain.Lifecycle, error)
		wantStatus transferdomain.Status
		wantReason string
	}{
		{
			name: "approve",
			transition: func(lifecycle transferdomain.Lifecycle) (transferdomain.Lifecycle, error) {
				return lifecycle.Approve()
			},
			wantStatus: transferdomain.StatusApproved,
		},
		{name: "require review", transition: func(lifecycle transferdomain.Lifecycle) (transferdomain.Lifecycle, error) {
			return lifecycle.RequireReview()
		}, wantStatus: transferdomain.StatusReviewRequired},
		{
			name: "decline",
			transition: func(lifecycle transferdomain.Lifecycle) (transferdomain.Lifecycle, error) {
				return lifecycle.Decline()
			},
			wantStatus: transferdomain.StatusDeclined,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := test.transition(mustPendingLifecycle(t))
			if err != nil {
				t.Fatalf("transition() error = %v", err)
			}
			if got.Status() != test.wantStatus || got.Version() != 2 ||
				got.FailureReason() != test.wantReason || got.JournalEntryID() != "" {
				t.Errorf(
					"transition() = (%s, %d, %q, %q), want (%s, 2, %q, empty journal ID)",
					got.Status(),
					got.Version(),
					got.FailureReason(),
					got.JournalEntryID(),
					test.wantStatus,
					test.wantReason,
				)
			}
		})
	}
}

func TestLifecycleResolvesManualReview(t *testing.T) {
	t.Parallel()
	review, err := mustPendingLifecycle(t).RequireReview()
	if err != nil {
		t.Fatalf("RequireReview() error = %v", err)
	}
	approved, err := review.Approve()
	if err != nil {
		t.Fatalf("Approve() after review error = %v", err)
	}
	if approved.Status() != transferdomain.StatusApproved || approved.Version() != 3 {
		t.Errorf("Approve() after review = (%s, %d), want (approved, 3)", approved.Status(), approved.Version())
	}
	declined, err := review.Decline()
	if err != nil {
		t.Fatalf("Decline() after review error = %v", err)
	}
	if declined.Status() != transferdomain.StatusDeclined || declined.Version() != 3 {
		t.Errorf("Decline() after review = (%s, %d), want (declined, 3)", declined.Status(), declined.Version())
	}
}

func TestLifecycleCompletesAndFailsOnlyAfterApproval(t *testing.T) {
	t.Parallel()
	approved := mustApprovedLifecycle(t)
	completed, err := approved.Complete()
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if completed.Status() != transferdomain.StatusCompleted || completed.Version() != 3 ||
		completed.JournalEntryID() != approved.Transfer().ID() {
		t.Errorf(
			"Complete() = (%s, %d, %q), want (completed, 3, %q)",
			completed.Status(),
			completed.Version(),
			completed.JournalEntryID(),
			approved.Transfer().ID(),
		)
	}
	failed, err := approved.Fail("insufficient_funds")
	if err != nil {
		t.Fatalf("Fail() error = %v", err)
	}
	if failed.Status() != transferdomain.StatusFailed || failed.Version() != 3 ||
		failed.FailureReason() != "insufficient_funds" {
		t.Errorf(
			"Fail() = (%s, %d, %q), want (failed, 3, insufficient_funds)",
			failed.Status(),
			failed.Version(),
			failed.FailureReason(),
		)
	}
}

func TestLifecycleRejectsInvalidTransitions(t *testing.T) {
	t.Parallel()
	pending := mustPendingLifecycle(t)
	if _, err := pending.Complete(); !errors.Is(err, transferdomain.ErrInvalidTransition) {
		t.Errorf("pending.Complete() error = %v, want ErrInvalidTransition", err)
	}
	if _, err := pending.Fail("insufficient_funds"); !errors.Is(err, transferdomain.ErrInvalidTransition) {
		t.Errorf("pending.Fail() error = %v, want ErrInvalidTransition", err)
	}
	approved := mustApprovedLifecycle(t)
	if _, err := approved.Fail("not valid"); !errors.Is(err, transferdomain.ErrInvalidLifecycle) {
		t.Errorf("approved.Fail() error = %v, want ErrInvalidLifecycle", err)
	}
}

func TestNewLifecycleValidatesPersistedState(t *testing.T) {
	t.Parallel()
	transfer := mustTransfer(t)
	tests := []struct {
		name   string
		params transferdomain.LifecycleParams
	}{
		{name: "pending version is not one", params: transferdomain.LifecycleParams{Transfer: transfer, Status: transferdomain.StatusPendingRisk, Version: 2, RiskPolicyVersion: "risk-v1"}},
		{name: "completed journal mismatch", params: transferdomain.LifecycleParams{Transfer: transfer, Status: transferdomain.StatusCompleted, Version: 2, RiskPolicyVersion: "risk-v1", JournalEntryID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}},
		{name: "failed missing reason", params: transferdomain.LifecycleParams{Transfer: transfer, Status: transferdomain.StatusFailed, Version: 2, RiskPolicyVersion: "risk-v1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := transferdomain.NewLifecycle(test.params); !errors.Is(err, transferdomain.ErrInvalidLifecycle) {
				t.Fatalf("NewLifecycle() error = %v, want ErrInvalidLifecycle", err)
			}
		})
	}
}

func TestNewLifecycleAcceptsHistoricalCompletedTransfer(t *testing.T) {
	transfer := mustTransfer(t)
	lifecycle, err := transferdomain.NewLifecycle(transferdomain.LifecycleParams{
		Transfer:       transfer,
		Status:         transferdomain.StatusCompleted,
		Version:        1,
		JournalEntryID: transfer.ID(),
	})
	if err != nil {
		t.Fatalf("NewLifecycle() error = %v", err)
	}
	if lifecycle.Status() != transferdomain.StatusCompleted || lifecycle.RiskPolicyVersion() != "" {
		t.Errorf("NewLifecycle() = (%s, %q), want historical completed lifecycle", lifecycle.Status(), lifecycle.RiskPolicyVersion())
	}
}

func mustPendingLifecycle(t testing.TB) transferdomain.Lifecycle {
	t.Helper()
	lifecycle, err := transferdomain.NewPendingLifecycle(mustTransfer(t), "risk-v1")
	if err != nil {
		t.Fatalf("NewPendingLifecycle() error = %v", err)
	}
	return lifecycle
}

func mustApprovedLifecycle(t testing.TB) transferdomain.Lifecycle {
	t.Helper()
	approved, err := mustPendingLifecycle(t).Approve()
	if err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	return approved
}

func mustTransfer(t testing.TB) transferdomain.Transfer {
	t.Helper()
	transfer, err := transferdomain.NewTransfer(validParams(t))
	if err != nil {
		t.Fatalf("NewTransfer() error = %v", err)
	}
	return transfer
}
