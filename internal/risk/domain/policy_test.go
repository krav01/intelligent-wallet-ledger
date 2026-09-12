package domain_test

import (
	"errors"
	"testing"

	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	riskdomain "github.com/krav01/intelligent-wallet-ledger/internal/risk/domain"
)

func TestNewPolicyRejectsInvalidParameters(t *testing.T) {
	t.Parallel()
	usd := mustCurrency(t, "USD")
	tests := []struct {
		name   string
		params riskdomain.PolicyParams
	}{
		{name: "missing version", params: riskdomain.PolicyParams{Thresholds: []riskdomain.Threshold{{Currency: usd, ReviewAmountMinor: 100, DeclineAmountMinor: 1_000}}}},
		{name: "missing thresholds", params: riskdomain.PolicyParams{Version: "risk-v1"}},
		{name: "invalid review amount", params: riskdomain.PolicyParams{Version: "risk-v1", Thresholds: []riskdomain.Threshold{{Currency: usd, DeclineAmountMinor: 1_000}}}},
		{name: "decline below review", params: riskdomain.PolicyParams{Version: "risk-v1", Thresholds: []riskdomain.Threshold{{Currency: usd, ReviewAmountMinor: 1_000, DeclineAmountMinor: 100}}}},
		{name: "negative velocity threshold", params: riskdomain.PolicyParams{Version: "risk-v1", Thresholds: []riskdomain.Threshold{{Currency: usd, ReviewAmountMinor: 100, DeclineAmountMinor: 1_000, VelocityReviewTransferCount: -1}}}},
		{name: "duplicate currency", params: riskdomain.PolicyParams{Version: "risk-v1", Thresholds: []riskdomain.Threshold{{Currency: usd, ReviewAmountMinor: 100, DeclineAmountMinor: 1_000}, {Currency: usd, ReviewAmountMinor: 200, DeclineAmountMinor: 2_000}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := riskdomain.NewPolicy(test.params); !errors.Is(err, riskdomain.ErrInvalidPolicy) {
				t.Fatalf("NewPolicy() error = %v, want ErrInvalidPolicy", err)
			}
		})
	}
}

func TestPolicyEvaluate(t *testing.T) {
	t.Parallel()
	policy := mustPolicy(t)
	tests := []struct {
		name           string
		currency       string
		amountMinor    int64
		wantScore      int
		wantDecision   riskdomain.Decision
		wantSignalCode riskdomain.SignalCode
	}{
		{name: "below review threshold", currency: "USD", amountMinor: 99, wantScore: 0, wantDecision: riskdomain.DecisionApprove},
		{name: "at review threshold", currency: "USD", amountMinor: 100, wantScore: 600, wantDecision: riskdomain.DecisionReview, wantSignalCode: riskdomain.SignalAmountReviewThreshold},
		{name: "at decline threshold", currency: "USD", amountMinor: 1_000, wantScore: 1_000, wantDecision: riskdomain.DecisionDecline, wantSignalCode: riskdomain.SignalAmountDeclineThreshold},
		{name: "missing currency policy", currency: "EUR", amountMinor: 99, wantScore: 500, wantDecision: riskdomain.DecisionReview, wantSignalCode: riskdomain.SignalCurrencyPolicyMissing},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			evaluation, err := policy.Evaluate(riskdomain.Input{Amount: mustMoney(t, test.amountMinor, test.currency)})
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			if evaluation.PolicyVersion() != "risk-v1" || evaluation.Score() != test.wantScore || evaluation.Decision() != test.wantDecision {
				t.Errorf("Evaluate() = (%q, %d, %s), want (risk-v1, %d, %s)", evaluation.PolicyVersion(), evaluation.Score(), evaluation.Decision(), test.wantScore, test.wantDecision)
			}
			signals := evaluation.Signals()
			if test.wantSignalCode == "" {
				if len(signals) != 0 {
					t.Errorf("Signals() = %+v, want none", signals)
				}
				return
			}
			if len(signals) != 1 || signals[0].Code() != test.wantSignalCode || signals[0].Contribution() != test.wantScore {
				t.Errorf("Signals() = %+v, want %s with contribution %d", signals, test.wantSignalCode, test.wantScore)
			}
		})
	}
}

func TestPolicyEvaluateRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	policy := mustPolicy(t)
	for _, input := range []riskdomain.Input{
		{},
		{Amount: mustMoney(t, 1, "USD"), VelocityTransferCount: -1},
	} {
		if _, err := policy.Evaluate(input); !errors.Is(err, riskdomain.ErrInvalidInput) {
			t.Fatalf("Evaluate() error = %v, want ErrInvalidInput", err)
		}
	}
}

func TestPolicyEvaluateVelocity(t *testing.T) {
	t.Parallel()
	policy, err := riskdomain.NewPolicy(riskdomain.PolicyParams{Version: "risk-v1", Thresholds: []riskdomain.Threshold{{Currency: mustCurrency(t, "USD"), ReviewAmountMinor: 100, DeclineAmountMinor: 1_000, VelocityReviewTransferCount: 3}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		input  riskdomain.Input
		signal riskdomain.SignalCode
	}{
		{"degraded", riskdomain.Input{Amount: mustMoney(t, 99, "USD"), VelocityDegraded: true}, riskdomain.SignalVelocityUnavailable},
		{"degraded missing policy", riskdomain.Input{Amount: mustMoney(t, 99, "EUR"), VelocityDegraded: true}, riskdomain.SignalVelocityUnavailable},
		{"threshold", riskdomain.Input{Amount: mustMoney(t, 99, "USD"), VelocityTransferCount: 3}, riskdomain.SignalVelocityReviewThreshold},
		{"decline takes precedence", riskdomain.Input{Amount: mustMoney(t, 1_000, "USD"), VelocityTransferCount: 3}, riskdomain.SignalAmountDeclineThreshold},
	} {
		t.Run(test.name, func(t *testing.T) {
			evaluation, err := policy.Evaluate(test.input)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			wantDecision := riskdomain.DecisionReview
			if test.signal == riskdomain.SignalAmountDeclineThreshold {
				wantDecision = riskdomain.DecisionDecline
			}
			if got := evaluation.Decision(); got != wantDecision {
				t.Errorf("Decision() = %s, want %s", got, wantDecision)
			}
			signals := evaluation.Signals()
			if len(signals) != 1 || signals[0].Code() != test.signal {
				t.Errorf("Signals() = %+v, want one %s signal", signals, test.signal)
			}
		})
	}
}

func TestEvaluationSignalsAreDefensive(t *testing.T) {
	t.Parallel()
	evaluation, err := mustPolicy(t).Evaluate(riskdomain.Input{Amount: mustMoney(t, 100, "USD")})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	first := evaluation.Signals()
	first[0] = riskdomain.Signal{}
	if got := evaluation.Signals()[0].Code(); got != riskdomain.SignalAmountReviewThreshold {
		t.Errorf("Signals() after caller mutation = %q, want review threshold signal", got)
	}
}

func mustPolicy(t testing.TB) riskdomain.Policy {
	t.Helper()
	policy, err := riskdomain.NewPolicy(riskdomain.PolicyParams{
		Version: "risk-v1",
		Thresholds: []riskdomain.Threshold{{
			Currency:           mustCurrency(t, "USD"),
			ReviewAmountMinor:  100,
			DeclineAmountMinor: 1_000,
		}},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	return policy
}

func mustCurrency(t testing.TB, code string) ledgerdomain.Currency {
	t.Helper()
	currency, err := ledgerdomain.ParseCurrency(code)
	if err != nil {
		t.Fatalf("ParseCurrency(%q) error = %v", code, err)
	}
	return currency
}

func mustMoney(t testing.TB, minorUnits int64, currencyCode string) ledgerdomain.Money {
	t.Helper()
	amount, err := ledgerdomain.NewMoney(minorUnits, mustCurrency(t, currencyCode))
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	return amount
}
