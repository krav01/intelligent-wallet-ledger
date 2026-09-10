// Package postgres persists consumer-scoped event deduplication in PostgreSQL.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/krav01/intelligent-wallet-ledger/internal/event"
)

const (
	maxConsumerNameBytes = 128

	insertInboxQuery = `
INSERT INTO consumer_inbox (consumer_name, event_id, event_type, event_version)
VALUES ($1, $2, $3, $4)
ON CONFLICT (consumer_name, event_id) DO NOTHING
RETURNING event_id`
)

// ErrInvalidArgument indicates malformed inbox input.
var ErrInvalidArgument = errors.New("inbox repository: invalid argument")

// ReserveTx reserves an event for one consumer inside a caller-owned transaction.
// The caller must commit the reservation with its durable side effect.
func ReserveTx(
	ctx context.Context,
	tx pgx.Tx,
	consumerName string,
	envelope event.Envelope,
) (bool, error) {
	if tx == nil {
		return false, fmt.Errorf("%w: transaction is required", ErrInvalidArgument)
	}
	if !validConsumerName(consumerName) {
		return false, fmt.Errorf("%w: consumer name is invalid", ErrInvalidArgument)
	}
	if envelope.EventID() == "" || envelope.EventType() == "" || envelope.EventVersion() <= 0 {
		return false, fmt.Errorf("%w: event envelope is invalid", ErrInvalidArgument)
	}

	var eventID string
	err := tx.QueryRow(
		ctx,
		insertInboxQuery,
		consumerName,
		envelope.EventID(),
		envelope.EventType(),
		envelope.EventVersion(),
	).Scan(&eventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reserving consumer event: %w", err)
	}

	return true, nil
}

func validConsumerName(value string) bool {
	if value == "" || len(value) > maxConsumerNameBytes {
		return false
	}
	for index, character := range []byte(value) {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
			continue
		}
		if index > 0 && (character == '.' || character == '_' || character == '-') {
			continue
		}
		return false
	}
	return true
}
