// Package postgres persists and leases transactional outbox events in PostgreSQL.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/krav01/intelligent-wallet-ledger/internal/event"
)

var (
	// ErrInvalidArgument indicates malformed outbox input.
	ErrInvalidArgument = errors.New("outbox repository: invalid argument")
	// ErrAlreadyExists indicates duplicate aggregate event identity.
	ErrAlreadyExists = errors.New("outbox repository: event already exists")
	// ErrClaimLost indicates that another publisher owns or completed the event.
	ErrClaimLost = errors.New("outbox repository: claim lost")
	// ErrCorruptData indicates that stored rows violate event invariants.
	ErrCorruptData = errors.New("outbox repository: corrupt data")
)

const insertEventQuery = `
INSERT INTO outbox_events (
    event_type,
    event_version,
    aggregate_type,
    aggregate_id,
    aggregate_version,
    correlation_id,
    causation_id,
    occurred_at,
    payload
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING event_id::text`

// AddTx stores an event inside a caller-owned transaction.
// The caller owns transaction commit, rollback, isolation, and retries.
// PostgreSQL-specific JSONB rejections are returned as ErrInvalidArgument.
func AddTx(ctx context.Context, tx pgx.Tx, draft event.Draft) (event.Envelope, error) {
	if tx == nil {
		return event.Envelope{}, fmt.Errorf("%w: transaction is required", ErrInvalidArgument)
	}
	validated, err := validateDraft(draft)
	if err != nil {
		return event.Envelope{}, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}

	var eventID string
	err = tx.QueryRow(
		ctx,
		insertEventQuery,
		validated.EventType(),
		validated.EventVersion(),
		validated.AggregateType(),
		validated.AggregateID(),
		validated.AggregateVersion(),
		validated.CorrelationID(),
		nullableString(validated.CausationID()),
		validated.OccurredAt(),
		[]byte(validated.Payload()),
	).Scan(&eventID)
	if err != nil {
		return event.Envelope{}, classifyWriteError(fmt.Errorf("inserting outbox event: %w", err))
	}

	envelope, err := event.NewEnvelope(eventID, validated)
	if err != nil {
		return event.Envelope{}, fmt.Errorf("%w: restored event: %w", ErrCorruptData, err)
	}

	return envelope, nil
}

func validateDraft(draft event.Draft) (event.Draft, error) {
	return event.NewDraft(event.DraftParams{
		EventType:        draft.EventType(),
		EventVersion:     draft.EventVersion(),
		AggregateType:    draft.AggregateType(),
		AggregateID:      draft.AggregateID(),
		AggregateVersion: draft.AggregateVersion(),
		CorrelationID:    draft.CorrelationID(),
		CausationID:      draft.CausationID(),
		OccurredAt:       draft.OccurredAt(),
		Payload:          draft.Payload(),
	})
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func classifyWriteError(err error) error {
	postgresError, ok := errors.AsType[*pgconn.PgError](err)
	if !ok {
		return err
	}
	if postgresError.Code == "23505" &&
		postgresError.ConstraintName == "outbox_events_aggregate_event_key" {
		return fmt.Errorf("%w: %w", ErrAlreadyExists, err)
	}
	if postgresError.Code == "23514" &&
		postgresError.ConstraintName == "outbox_events_payload_check" {
		return fmt.Errorf("%w: stored payload exceeds PostgreSQL JSONB limit: %w", ErrInvalidArgument, err)
	}
	if strings.HasPrefix(postgresError.Code, "22") {
		return fmt.Errorf("%w: PostgreSQL rejected event data: %w", ErrInvalidArgument, err)
	}

	return err
}
