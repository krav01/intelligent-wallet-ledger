// Package events maps transfer domain outcomes to integration event contracts.
package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
)

const (
	// CompletedType is the stable transfer completion event name.
	CompletedType = "transfer.completed"
	// CompletedVersion is the current transfer completion payload version.
	CompletedVersion int16 = 1
)

// Completed creates the version 1 integration event for a committed transfer.
func Completed(transfer transferdomain.Transfer) (event.Draft, error) {
	if _, err := transfer.JournalEntry(); err != nil {
		return event.Draft{}, fmt.Errorf("creating completed transfer event: %w", err)
	}

	payload, err := json.Marshal(struct {
		TransferID           string `json:"transfer_id"`
		RequesterID          string `json:"requester_id"`
		SourceAccountID      string `json:"source_account_id"`
		DestinationAccountID string `json:"destination_account_id"`
		Currency             string `json:"currency"`
		AmountMinor          int64  `json:"amount_minor"`
		RequestedAt          string `json:"requested_at"`
	}{
		TransferID:           transfer.ID(),
		RequesterID:          transfer.RequesterID(),
		SourceAccountID:      transfer.SourceAccountID(),
		DestinationAccountID: transfer.DestinationAccountID(),
		Currency:             transfer.Amount().Currency().String(),
		AmountMinor:          transfer.Amount().MinorUnits(),
		RequestedAt:          transfer.RequestedAt().Format(time.RFC3339Nano),
	})
	if err != nil {
		return event.Draft{}, fmt.Errorf("encoding completed transfer payload: %w", err)
	}

	draft, err := event.NewDraft(event.DraftParams{
		EventType:        CompletedType,
		EventVersion:     CompletedVersion,
		AggregateType:    "transfer",
		AggregateID:      transfer.ID(),
		AggregateVersion: 1,
		CorrelationID:    transfer.ID(),
		OccurredAt:       transfer.RequestedAt(),
		Payload:          payload,
	})
	if err != nil {
		return event.Draft{}, fmt.Errorf("creating completed transfer envelope: %w", err)
	}

	return draft, nil
}

// CompletedLifecycle creates the version 1 event for a lifecycle completed by an asynchronous worker.
func CompletedLifecycle(
	lifecycle transferdomain.Lifecycle,
	completedAt time.Time,
	causationID string,
) (event.Draft, error) {
	if lifecycle.Status() != transferdomain.StatusCompleted || causationID == "" {
		return event.Draft{}, fmt.Errorf("creating completed lifecycle event: %w", transferdomain.ErrInvalidLifecycle)
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
	}{
		TransferID:           transfer.ID(),
		RequesterID:          transfer.RequesterID(),
		SourceAccountID:      transfer.SourceAccountID(),
		DestinationAccountID: transfer.DestinationAccountID(),
		Currency:             transfer.Amount().Currency().String(),
		AmountMinor:          transfer.Amount().MinorUnits(),
		RequestedAt:          transfer.RequestedAt().Format(time.RFC3339Nano),
	})
	if err != nil {
		return event.Draft{}, fmt.Errorf("encoding completed transfer payload: %w", err)
	}

	draft, err := event.NewDraft(event.DraftParams{
		EventType:        CompletedType,
		EventVersion:     CompletedVersion,
		AggregateType:    "transfer",
		AggregateID:      transfer.ID(),
		AggregateVersion: lifecycle.Version(),
		CorrelationID:    transfer.ID(),
		CausationID:      causationID,
		OccurredAt:       completedAt,
		Payload:          payload,
	})
	if err != nil {
		return event.Draft{}, fmt.Errorf("creating completed transfer envelope: %w", err)
	}

	return draft, nil
}
