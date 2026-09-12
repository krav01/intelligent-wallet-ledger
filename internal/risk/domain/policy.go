// Package domain contains deterministic risk-assessment invariants.
package domain

import (
	"errors"
	"fmt"
	"maps"
	"strings"

	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
)

const (
	maxPolicyVersionBytes = 64
	maxScore              = 1000
	reviewScore           = 600
	missingPolicyScore    = 500
	velocityReviewScore   = 700
)

var (
	// ErrInvalidPolicy indicates malformed deterministic risk policy configuration.
	ErrInvalidPolicy = errors.New("risk domain: invalid policy")
	// ErrInvalidInput indicates malformed risk-evaluation input.
	ErrInvalidInput = errors.New("risk domain: invalid input")
)

// Decision is the deterministic result of a risk assessment.
type Decision uint8

const (
	// DecisionUnknown is the invalid zero decision.
	DecisionUnknown Decision = iota
	// DecisionApprove permits the transfer to proceed to posting.
	DecisionApprove
	// DecisionReview requires a later manual decision.
	DecisionReview
	// DecisionDecline prevents the transfer from posting.
	DecisionDecline
)

// String returns the stable wire representation of a decision.
func (d Decision) String() string {
	switch d {
	case DecisionApprove:
		return "approve"
	case DecisionReview:
		return "review"
	case DecisionDecline:
		return "decline"
	default:
		return ""
	}
}

// SignalCode identifies a stable, explainable deterministic rule result.
type SignalCode string

const (
	// SignalAmountReviewThreshold indicates the amount met its review threshold.
	SignalAmountReviewThreshold SignalCode = "amount_review_threshold"
	// SignalAmountDeclineThreshold indicates the amount met its decline threshold.
	SignalAmountDeclineThreshold SignalCode = "amount_decline_threshold"
	// SignalCurrencyPolicyMissing indicates no explicit policy exists for a currency.
	SignalCurrencyPolicyMissing SignalCode = "currency_policy_missing"
	// SignalVelocityUnavailable indicates the captured velocity observation was unavailable.
	SignalVelocityUnavailable SignalCode = "velocity_unavailable"
	// SignalVelocityReviewThreshold indicates the captured transfer count requires review.
	SignalVelocityReviewThreshold SignalCode = "velocity_review_threshold"
)

// Threshold defines deterministic risk thresholds for one currency.
type Threshold struct {
	Currency ledgerdomain.Currency
	// ReviewAmountMinor is the inclusive amount threshold requiring manual review.
	ReviewAmountMinor int64
	// DeclineAmountMinor is the inclusive amount threshold declining a transfer.
	DeclineAmountMinor int64
	// VelocityReviewTransferCount is the inclusive captured count threshold requiring review.
	VelocityReviewTransferCount int
}

// PolicyParams contains validated, versioned deterministic risk thresholds.
type PolicyParams struct {
	Version    string
	Thresholds []Threshold
}

// Policy is an immutable, versioned deterministic risk policy.
type Policy struct {
	version    string
	thresholds map[ledgerdomain.Currency]Threshold
}

// Input is the immutable transfer information required for one evaluation.
type Input struct {
	Amount ledgerdomain.Money
	// VelocityTransferCount is the non-negative count from a captured observation window.
	VelocityTransferCount int
	// VelocityDegraded reports that the captured velocity observation was unavailable.
	VelocityDegraded bool
}

// Signal records a stable rule code and its integer score contribution.
type Signal struct {
	code         SignalCode
	contribution int
}

// Code returns the stable rule code.
func (s Signal) Code() SignalCode { return s.code }

// Contribution returns the signal's contribution to the total score.
func (s Signal) Contribution() int { return s.contribution }

// Evaluation is the reproducible output of applying one policy to one input.
type Evaluation struct {
	policyVersion string
	score         int
	decision      Decision
	signals       []Signal
}

// NewPolicy validates and creates an immutable deterministic policy.
func NewPolicy(params PolicyParams) (Policy, error) {
	version := strings.TrimSpace(params.Version)
	if !validPolicyVersion(version) {
		return Policy{}, fmt.Errorf("%w: version", ErrInvalidPolicy)
	}
	if len(params.Thresholds) == 0 {
		return Policy{}, fmt.Errorf("%w: thresholds are required", ErrInvalidPolicy)
	}

	thresholds := make(map[ledgerdomain.Currency]Threshold, len(params.Thresholds))
	for _, threshold := range params.Thresholds {
		if threshold.Currency.String() == "" {
			return Policy{}, fmt.Errorf("%w: threshold currency", ErrInvalidPolicy)
		}
		if threshold.ReviewAmountMinor <= 0 || threshold.DeclineAmountMinor < threshold.ReviewAmountMinor || threshold.VelocityReviewTransferCount < 0 {
			return Policy{}, fmt.Errorf("%w: threshold amounts", ErrInvalidPolicy)
		}
		if _, exists := thresholds[threshold.Currency]; exists {
			return Policy{}, fmt.Errorf("%w: duplicate currency %s", ErrInvalidPolicy, threshold.Currency)
		}
		thresholds[threshold.Currency] = threshold
	}

	return Policy{version: version, thresholds: maps.Clone(thresholds)}, nil
}

// Version returns the immutable policy version.
func (p Policy) Version() string { return p.version }

// RequiresVelocity reports whether any threshold requires a captured velocity observation.
func (p Policy) RequiresVelocity() bool {
	for _, threshold := range p.thresholds {
		if threshold.VelocityReviewTransferCount > 0 {
			return true
		}
	}
	return false
}

// Evaluate produces a deterministic result without reading mutable process state.
func (p Policy) Evaluate(input Input) (Evaluation, error) {
	if err := p.validate(); err != nil {
		return Evaluation{}, err
	}
	if input.Amount.Currency().String() == "" || input.Amount.MinorUnits() <= 0 || input.VelocityTransferCount < 0 {
		return Evaluation{}, fmt.Errorf("%w: amount", ErrInvalidInput)
	}

	threshold, exists := p.thresholds[input.Amount.Currency()]
	if !exists {
		if input.VelocityDegraded {
			return newEvaluation(p.version, velocityReviewScore, DecisionReview, []Signal{{code: SignalVelocityUnavailable, contribution: velocityReviewScore}}), nil
		}
		return newEvaluation(
			p.version,
			missingPolicyScore,
			DecisionReview,
			[]Signal{{code: SignalCurrencyPolicyMissing, contribution: missingPolicyScore}},
		), nil
	}
	if input.Amount.MinorUnits() >= threshold.DeclineAmountMinor {
		return newEvaluation(
			p.version,
			maxScore,
			DecisionDecline,
			[]Signal{{code: SignalAmountDeclineThreshold, contribution: maxScore}},
		), nil
	}
	if input.VelocityDegraded {
		return newEvaluation(p.version, velocityReviewScore, DecisionReview, []Signal{{code: SignalVelocityUnavailable, contribution: velocityReviewScore}}), nil
	}
	if threshold.VelocityReviewTransferCount > 0 && input.VelocityTransferCount >= threshold.VelocityReviewTransferCount {
		return newEvaluation(p.version, velocityReviewScore, DecisionReview, []Signal{{code: SignalVelocityReviewThreshold, contribution: velocityReviewScore}}), nil
	}
	if input.Amount.MinorUnits() >= threshold.ReviewAmountMinor {
		return newEvaluation(
			p.version,
			reviewScore,
			DecisionReview,
			[]Signal{{code: SignalAmountReviewThreshold, contribution: reviewScore}},
		), nil
	}

	return newEvaluation(p.version, 0, DecisionApprove, []Signal{}), nil
}

// PolicyVersion returns the version used for the evaluation.
func (e Evaluation) PolicyVersion() string { return e.policyVersion }

// Score returns an integer from 0 through 1000.
func (e Evaluation) Score() int { return e.score }

// Decision returns the deterministic risk decision.
func (e Evaluation) Decision() Decision { return e.decision }

// Signals returns a defensive copy in deterministic order.
func (e Evaluation) Signals() []Signal { return append([]Signal(nil), e.signals...) }

func newEvaluation(version string, score int, decision Decision, signals []Signal) Evaluation {
	return Evaluation{
		policyVersion: version,
		score:         score,
		decision:      decision,
		signals:       append([]Signal(nil), signals...),
	}
}

func (p Policy) validate() error {
	if !validPolicyVersion(p.version) || len(p.thresholds) == 0 {
		return ErrInvalidPolicy
	}
	for currency, threshold := range p.thresholds {
		if currency.String() == "" || threshold.Currency != currency ||
			threshold.ReviewAmountMinor <= 0 || threshold.DeclineAmountMinor < threshold.ReviewAmountMinor || threshold.VelocityReviewTransferCount < 0 {
			return ErrInvalidPolicy
		}
	}
	return nil
}

func validPolicyVersion(value string) bool {
	if len(value) == 0 || len(value) > maxPolicyVersionBytes {
		return false
	}
	for _, character := range value {
		if character < '!' || character > '~' {
			return false
		}
	}
	return true
}
