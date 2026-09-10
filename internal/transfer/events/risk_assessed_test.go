package events_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	riskdomain "github.com/krav01/intelligent-wallet-ledger/internal/risk/domain"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
	transferevents "github.com/krav01/intelligent-wallet-ledger/internal/transfer/events"
)

func TestRiskAssessed(t *testing.T) {
	t.Parallel()
	pending := mustPendingLifecycle(t)
	lifecycle, err := pending.RequireReview()
	if err != nil {
		t.Fatalf("RequireReview() error = %v", err)
	}
	evaluation := mustReviewEvaluation(t)
	assessedAt := time.Date(2026, time.September, 11, 12, 1, 0, 0, time.UTC)
	causationID := "55555555-5555-4555-8555-555555555555"

	draft, err := transferevents.RiskAssessed(lifecycle, evaluation, assessedAt, causationID)
	if err != nil {
		t.Fatalf("RiskAssessed() error = %v", err)
	}
	if draft.EventType() != transferevents.RiskAssessedType ||
		draft.EventVersion() != transferevents.RiskAssessedVersion ||
		draft.AggregateID() != lifecycle.Transfer().ID() ||
		draft.AggregateVersion() != lifecycle.Version() ||
		draft.CorrelationID() != lifecycle.Transfer().ID() ||
		draft.CausationID() != causationID || !draft.OccurredAt().Equal(assessedAt) {
		t.Errorf("RiskAssessed() metadata does not match lifecycle")
	}

	var payload struct {
		TransferID        string `json:"transfer_id"`
		RiskPolicyVersion string `json:"risk_policy_version"`
		Score             int    `json:"score"`
		Decision          string `json:"decision"`
		Signals           []struct {
			Code         string `json:"code"`
			Contribution int    `json:"contribution"`
		} `json:"signals"`
	}
	if err := json.Unmarshal(draft.Payload(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if payload.TransferID != lifecycle.Transfer().ID() || payload.RiskPolicyVersion != "risk-v1" ||
		payload.Score != 600 || payload.Decision != "review" || len(payload.Signals) != 1 ||
		payload.Signals[0].Code != "amount_review_threshold" || payload.Signals[0].Contribution != 600 {
		t.Errorf("RiskAssessed() payload = %+v, want deterministic review result", payload)
	}
}

func TestRiskAssessedRejectsMismatchedLifecycle(t *testing.T) {
	t.Parallel()
	lifecycle, err := mustPendingLifecycle(t).Approve()
	if err != nil {
		t.Fatalf("Approve() error = %v", err)
	}

	_, err = transferevents.RiskAssessed(
		lifecycle,
		mustReviewEvaluation(t),
		time.Date(2026, time.September, 11, 12, 1, 0, 0, time.UTC),
		"55555555-5555-4555-8555-555555555555",
	)
	if !errors.Is(err, transferdomain.ErrInvalidLifecycle) {
		t.Fatalf("RiskAssessed() error = %v, want ErrInvalidLifecycle", err)
	}
}

func TestRiskAssessedRejectsMissingCausationID(t *testing.T) {
	t.Parallel()
	lifecycle, err := mustPendingLifecycle(t).RequireReview()
	if err != nil {
		t.Fatalf("RequireReview() error = %v", err)
	}

	_, err = transferevents.RiskAssessed(
		lifecycle,
		mustReviewEvaluation(t),
		time.Date(2026, time.September, 11, 12, 1, 0, 0, time.UTC),
		"",
	)
	if !errors.Is(err, transferdomain.ErrInvalidLifecycle) {
		t.Fatalf("RiskAssessed() error = %v, want ErrInvalidLifecycle", err)
	}
}

func mustReviewEvaluation(t testing.TB) riskdomain.Evaluation {
	t.Helper()
	currency, err := ledgerdomain.ParseCurrency("USD")
	if err != nil {
		t.Fatalf("ParseCurrency() error = %v", err)
	}
	policy, err := riskdomain.NewPolicy(riskdomain.PolicyParams{
		Version: "risk-v1",
		Thresholds: []riskdomain.Threshold{{
			Currency:           currency,
			ReviewAmountMinor:  100,
			DeclineAmountMinor: 200,
		}},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	money, err := ledgerdomain.NewMoney(125, currency)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	evaluation, err := policy.Evaluate(riskdomain.Input{Amount: money})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	return evaluation
}
