package events

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
)

const (
	// RequestedType is the stable transfer-request event name.
	RequestedType = "transfer.requested"
	// RequestedVersion is the current transfer-request payload version.
	RequestedVersion int16 = 1
)

// ParseRequested strictly decodes a version 1 transfer.requested envelope.
func ParseRequested(envelope event.Envelope) (transferdomain.Lifecycle, error) {
	if envelope.EventType() != RequestedType || envelope.EventVersion() != RequestedVersion ||
		envelope.AggregateType() != "transfer" || envelope.AggregateVersion() != 1 ||
		envelope.CausationID() != "" {
		return transferdomain.Lifecycle{}, fmt.Errorf("parsing requested transfer: %w", event.ErrInvalidEnvelope)
	}

	var payload struct {
		TransferID           string `json:"transfer_id"`
		RequesterID          string `json:"requester_id"`
		SourceAccountID      string `json:"source_account_id"`
		DestinationAccountID string `json:"destination_account_id"`
		Currency             string `json:"currency"`
		AmountMinor          int64  `json:"amount_minor"`
		RequestedAt          string `json:"requested_at"`
		RiskPolicyVersion    string `json:"risk_policy_version"`
	}
	decoder := json.NewDecoder(bytes.NewReader(envelope.Payload()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return transferdomain.Lifecycle{}, fmt.Errorf("parsing requested transfer payload: %w", event.ErrInvalidEnvelope)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return transferdomain.Lifecycle{}, fmt.Errorf("parsing requested transfer payload: %w", event.ErrInvalidEnvelope)
	}
	if payload.TransferID != envelope.AggregateID() || envelope.CorrelationID() != payload.TransferID {
		return transferdomain.Lifecycle{}, fmt.Errorf("parsing requested transfer payload: %w", event.ErrInvalidEnvelope)
	}

	currency, err := ledgerdomain.ParseCurrency(payload.Currency)
	if err != nil {
		return transferdomain.Lifecycle{}, fmt.Errorf("parsing requested transfer currency: %w", event.ErrInvalidEnvelope)
	}
	amount, err := ledgerdomain.NewMoney(payload.AmountMinor, currency)
	if err != nil {
		return transferdomain.Lifecycle{}, fmt.Errorf("parsing requested transfer amount: %w", event.ErrInvalidEnvelope)
	}
	requestedAt, err := time.Parse(time.RFC3339Nano, payload.RequestedAt)
	if err != nil {
		return transferdomain.Lifecycle{}, fmt.Errorf("parsing requested transfer time: %w", event.ErrInvalidEnvelope)
	}
	transfer, err := transferdomain.NewTransfer(transferdomain.NewTransferParams{
		ID:                   payload.TransferID,
		IdempotencyKey:       "event-request",
		RequesterID:          payload.RequesterID,
		SourceAccountID:      payload.SourceAccountID,
		DestinationAccountID: payload.DestinationAccountID,
		Amount:               amount,
		RequestedAt:          requestedAt,
	})
	if err != nil {
		return transferdomain.Lifecycle{}, fmt.Errorf("parsing requested transfer intent: %w", event.ErrInvalidEnvelope)
	}
	lifecycle, err := transferdomain.NewPendingLifecycle(transfer, payload.RiskPolicyVersion)
	if err != nil {
		return transferdomain.Lifecycle{}, fmt.Errorf("parsing requested transfer lifecycle: %w", event.ErrInvalidEnvelope)
	}
	return lifecycle, nil
}

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
