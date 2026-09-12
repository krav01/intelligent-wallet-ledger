package events

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	riskdomain "github.com/krav01/intelligent-wallet-ledger/internal/risk/domain"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
)

const (
	// RiskAssessedType is the stable transfer-risk-result event name.
	RiskAssessedType = "transfer.risk_assessed"
	// RiskAssessedVersion is the current transfer-risk-result payload version.
	RiskAssessedVersion int16 = 1
)

// RiskAssessment is the posting-relevant, immutable result of a risk event.
type RiskAssessment struct {
	transferID        string
	riskPolicyVersion string
	decision          riskdomain.Decision
}

// TransferID returns the assessed transfer identity.
func (a RiskAssessment) TransferID() string { return a.transferID }

// RiskPolicyVersion returns the immutable policy used for the assessment.
func (a RiskAssessment) RiskPolicyVersion() string { return a.riskPolicyVersion }

// Decision returns the risk decision that controls whether posting may proceed.
func (a RiskAssessment) Decision() riskdomain.Decision { return a.decision }

// ParseRiskAssessed strictly decodes a version 1 transfer.risk_assessed envelope.
func ParseRiskAssessed(envelope event.Envelope) (RiskAssessment, error) {
	if envelope.EventType() != RiskAssessedType || envelope.EventVersion() != RiskAssessedVersion ||
		envelope.AggregateType() != "transfer" || envelope.AggregateVersion() != 2 ||
		envelope.CausationID() == "" {
		return RiskAssessment{}, fmt.Errorf("parsing risk assessed transfer: %w", event.ErrInvalidEnvelope)
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
	decoder := json.NewDecoder(bytes.NewReader(envelope.Payload()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return RiskAssessment{}, fmt.Errorf("parsing risk assessed payload: %w", event.ErrInvalidEnvelope)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return RiskAssessment{}, fmt.Errorf("parsing risk assessed payload: %w", event.ErrInvalidEnvelope)
	}
	if payload.TransferID != envelope.AggregateID() || envelope.CorrelationID() != payload.TransferID ||
		payload.RiskPolicyVersion == "" || payload.Score < 0 || payload.Score > 1000 {
		return RiskAssessment{}, fmt.Errorf("parsing risk assessed payload: %w", event.ErrInvalidEnvelope)
	}

	decision, ok := riskDecision(payload.Decision)
	if !ok {
		return RiskAssessment{}, fmt.Errorf("parsing risk assessed decision: %w", event.ErrInvalidEnvelope)
	}
	return RiskAssessment{
		transferID:        payload.TransferID,
		riskPolicyVersion: payload.RiskPolicyVersion,
		decision:          decision,
	}, nil
}

// RiskAssessed creates the version 1 event for a persisted deterministic risk decision.
func RiskAssessed(
	lifecycle transferdomain.Lifecycle,
	evaluation riskdomain.Evaluation,
	assessedAt time.Time,
	causationID string,
) (event.Draft, error) {
	if !matchesRiskDecision(lifecycle.Status(), evaluation.Decision()) ||
		evaluation.PolicyVersion() != lifecycle.RiskPolicyVersion() ||
		evaluation.Score() < 0 || evaluation.Score() > 1000 || causationID == "" {
		return event.Draft{}, fmt.Errorf("creating risk assessed event: %w", transferdomain.ErrInvalidLifecycle)
	}

	type signalPayload struct {
		Code         string `json:"code"`
		Contribution int    `json:"contribution"`
	}
	signals := evaluation.Signals()
	payloadSignals := make([]signalPayload, len(signals))
	for index, signal := range signals {
		payloadSignals[index] = signalPayload{
			Code:         string(signal.Code()),
			Contribution: signal.Contribution(),
		}
	}

	payload, err := json.Marshal(struct {
		TransferID        string          `json:"transfer_id"`
		RiskPolicyVersion string          `json:"risk_policy_version"`
		Score             int             `json:"score"`
		Decision          string          `json:"decision"`
		Signals           []signalPayload `json:"signals"`
	}{
		TransferID:        lifecycle.Transfer().ID(),
		RiskPolicyVersion: evaluation.PolicyVersion(),
		Score:             evaluation.Score(),
		Decision:          evaluation.Decision().String(),
		Signals:           payloadSignals,
	})
	if err != nil {
		return event.Draft{}, fmt.Errorf("encoding risk assessed payload: %w", err)
	}

	draft, err := event.NewDraft(event.DraftParams{
		EventType:        RiskAssessedType,
		EventVersion:     RiskAssessedVersion,
		AggregateType:    "transfer",
		AggregateID:      lifecycle.Transfer().ID(),
		AggregateVersion: lifecycle.Version(),
		CorrelationID:    lifecycle.Transfer().ID(),
		CausationID:      causationID,
		OccurredAt:       assessedAt,
		Payload:          payload,
	})
	if err != nil {
		return event.Draft{}, fmt.Errorf("creating risk assessed envelope: %w", err)
	}

	return draft, nil
}

func matchesRiskDecision(status transferdomain.Status, decision riskdomain.Decision) bool {
	switch decision {
	case riskdomain.DecisionApprove:
		return status == transferdomain.StatusApproved
	case riskdomain.DecisionReview:
		return status == transferdomain.StatusReviewRequired
	case riskdomain.DecisionDecline:
		return status == transferdomain.StatusDeclined
	default:
		return false
	}
}

func riskDecision(value string) (riskdomain.Decision, bool) {
	switch value {
	case riskdomain.DecisionApprove.String():
		return riskdomain.DecisionApprove, true
	case riskdomain.DecisionReview.String():
		return riskdomain.DecisionReview, true
	case riskdomain.DecisionDecline.String():
		return riskdomain.DecisionDecline, true
	default:
		return riskdomain.DecisionUnknown, false
	}
}
