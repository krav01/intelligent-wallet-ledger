package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
)

const (
	// RequestedType is the stable transfer-request event name.
	RequestedType = "transfer.requested"
	// RequestedVersion is the current transfer-request payload version.
	RequestedVersion int16 = 1
)

// Requested creates the version 1 integration event for an accepted transfer.
func Requested(lifecycle transferdomain.Lifecycle) (event.Draft, error) {
	if lifecycle.Status() != transferdomain.StatusPendingRisk {
		return event.Draft{}, fmt.Errorf("creating requested transfer event: %w", transferdomain.ErrInvalidLifecycle)
	}

	transfer := lifecycle.Transfer()
	payload, err := json.Marshal(struct {
		TransferID           string `json:"transfer_id"`
		RequesterID          string `json:"requester_id"`
		SourceAccountID      string `json:"source_account_id"`
		DestinationAccountID string `json:"destination_account_id"`
		Currency             string `json:"currency"`
		AmountMinor          int64  `json:"amount_minor"`
		RequestedAt          string `json:"requested_at"`
		RiskPolicyVersion    string `json:"risk_policy_version"`
	}{
		TransferID:           transfer.ID(),
		RequesterID:          transfer.RequesterID(),
		SourceAccountID:      transfer.SourceAccountID(),
		DestinationAccountID: transfer.DestinationAccountID(),
		Currency:             transfer.Amount().Currency().String(),
		AmountMinor:          transfer.Amount().MinorUnits(),
		RequestedAt:          transfer.RequestedAt().Format(time.RFC3339Nano),
		RiskPolicyVersion:    lifecycle.RiskPolicyVersion(),
	})
	if err != nil {
		return event.Draft{}, fmt.Errorf("encoding requested transfer payload: %w", err)
	}

	draft, err := event.NewDraft(event.DraftParams{
		EventType:        RequestedType,
		EventVersion:     RequestedVersion,
		AggregateType:    "transfer",
		AggregateID:      transfer.ID(),
		AggregateVersion: lifecycle.Version(),
		CorrelationID:    transfer.ID(),
		OccurredAt:       transfer.RequestedAt(),
		Payload:          payload,
	})
	if err != nil {
		return event.Draft{}, fmt.Errorf("creating requested transfer envelope: %w", err)
	}

	return draft, nil
}
