package events

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
)

const (
	// ReviewDecidedType is the stable manual-review outcome event name.
	ReviewDecidedType = "transfer.review_decided"
	// ReviewDecidedVersion is the current transfer.review_decided payload version.
	ReviewDecidedVersion int16 = 1
)

// ReviewDecision is the posting-relevant result of an analyst review.
type ReviewDecision struct {
	transferID string
	decision   string
}

// TransferID returns the reviewed transfer identity.
func (d ReviewDecision) TransferID() string { return d.transferID }

// Decision returns approved or declined.
func (d ReviewDecision) Decision() string { return d.decision }

// ParseReviewDecided strictly decodes a version 1 transfer.review_decided envelope.
func ParseReviewDecided(envelope event.Envelope) (ReviewDecision, error) {
	if envelope.EventType() != ReviewDecidedType || envelope.EventVersion() != ReviewDecidedVersion || envelope.AggregateType() != "transfer" || envelope.AggregateVersion() < 3 {
		return ReviewDecision{}, fmt.Errorf("parsing review decided transfer: %w", event.ErrInvalidEnvelope)
	}
	var payload struct {
		TransferID string `json:"transfer_id"`
		Decision   string `json:"decision"`
		DecidedBy  string `json:"decided_by"`
	}
	decoder := json.NewDecoder(bytes.NewReader(envelope.Payload()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return ReviewDecision{}, fmt.Errorf("parsing review decided payload: %w", event.ErrInvalidEnvelope)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ReviewDecision{}, fmt.Errorf("parsing review decided payload: %w", event.ErrInvalidEnvelope)
	}
	if payload.TransferID != envelope.AggregateID() || envelope.CorrelationID() != payload.TransferID || payload.DecidedBy == "" || (payload.Decision != "approved" && payload.Decision != "declined") {
		return ReviewDecision{}, fmt.Errorf("parsing review decided payload: %w", event.ErrInvalidEnvelope)
	}
	return ReviewDecision{transferID: payload.TransferID, decision: payload.Decision}, nil
}

// ReviewDecided creates the event that makes a manual transfer decision durable.
func ReviewDecided(lifecycle transferdomain.Lifecycle, subject string, decidedAt time.Time) (event.Draft, error) {
	if (lifecycle.Status() != transferdomain.StatusApproved && lifecycle.Status() != transferdomain.StatusDeclined) || subject == "" {
		return event.Draft{}, fmt.Errorf("creating review decided event: %w", transferdomain.ErrInvalidLifecycle)
	}
	payload, err := json.Marshal(struct {
		TransferID string `json:"transfer_id"`
		Decision   string `json:"decision"`
		DecidedBy  string `json:"decided_by"`
	}{lifecycle.Transfer().ID(), lifecycle.Status().String(), subject})
	if err != nil {
		return event.Draft{}, fmt.Errorf("encoding review decided payload: %w", err)
	}
	return event.NewDraft(event.DraftParams{EventType: ReviewDecidedType, EventVersion: ReviewDecidedVersion, AggregateType: "transfer", AggregateID: lifecycle.Transfer().ID(), AggregateVersion: lifecycle.Version(), CorrelationID: lifecycle.Transfer().ID(), OccurredAt: decidedAt.UTC(), Payload: payload})
}
