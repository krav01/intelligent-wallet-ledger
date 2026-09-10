package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
)

const (
	// FailedType is the stable transfer-failure event name.
	FailedType = "transfer.failed"
	// FailedVersion is the current transfer-failure payload version.
	FailedVersion int16 = 1
)

// Failed creates the version 1 event for a terminal transfer business failure.
func Failed(
	lifecycle transferdomain.Lifecycle,
	failedAt time.Time,
	causationID string,
) (event.Draft, error) {
	if lifecycle.Status() != transferdomain.StatusFailed || causationID == "" {
		return event.Draft{}, fmt.Errorf("creating failed transfer event: %w", transferdomain.ErrInvalidLifecycle)
	}

	payload, err := json.Marshal(struct {
		TransferID string `json:"transfer_id"`
		ReasonCode string `json:"reason_code"`
	}{
		TransferID: lifecycle.Transfer().ID(),
		ReasonCode: lifecycle.FailureReason(),
	})
	if err != nil {
		return event.Draft{}, fmt.Errorf("encoding failed transfer payload: %w", err)
	}

	draft, err := event.NewDraft(event.DraftParams{
		EventType:        FailedType,
		EventVersion:     FailedVersion,
		AggregateType:    "transfer",
		AggregateID:      lifecycle.Transfer().ID(),
		AggregateVersion: lifecycle.Version(),
		CorrelationID:    lifecycle.Transfer().ID(),
		CausationID:      causationID,
		OccurredAt:       failedAt,
		Payload:          payload,
	})
	if err != nil {
		return event.Draft{}, fmt.Errorf("creating failed transfer envelope: %w", err)
	}

	return draft, nil
}
