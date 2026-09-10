package events

import (
	"encoding/json"
	"fmt"
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
