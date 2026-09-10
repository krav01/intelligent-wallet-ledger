package postgres

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	"github.com/krav01/intelligent-wallet-ledger/internal/outbox"
)

const (
	maxClaimBatch      = 100
	minClaimLease      = time.Second
	maxClaimLease      = 15 * time.Minute
	maxRetryDelay      = 24 * time.Hour
	maxDiagnosticBytes = 1024
	fallbackDiagnostic = "publication failed"

	claimEventsQuery = `
WITH candidates AS (
    SELECT event_id
    FROM outbox_events
    WHERE published_at IS NULL
      AND available_at <= CURRENT_TIMESTAMP
      AND (claimed_until IS NULL OR claimed_until <= CURRENT_TIMESTAMP)
    ORDER BY available_at, sequence
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
UPDATE outbox_events AS event
SET claimed_by = $2,
    claimed_until = CURRENT_TIMESTAMP + make_interval(secs => $3)
FROM candidates
WHERE event.event_id = candidates.event_id
RETURNING
    event.event_id::text,
    event.sequence,
    event.event_type,
    event.event_version,
    event.aggregate_type,
    event.aggregate_id::text,
    event.aggregate_version,
    event.correlation_id::text,
    COALESCE(event.causation_id::text, ''),
    event.occurred_at,
    event.payload,
    event.publish_attempts`

	markPublishedQuery = `
UPDATE outbox_events
SET published_at = CURRENT_TIMESTAMP,
    claimed_by = NULL,
    claimed_until = NULL,
    last_error = NULL
WHERE event_id = $1
  AND claimed_by = $2
  AND published_at IS NULL`

	markFailedQuery = `
UPDATE outbox_events
SET available_at = CURRENT_TIMESTAMP + make_interval(secs => $3),
    publish_attempts = LEAST(publish_attempts::bigint + 1, 2147483647)::integer,
    claimed_by = NULL,
    claimed_until = NULL,
    last_error = $4
WHERE event_id = $1
  AND claimed_by = $2
  AND published_at IS NULL`
)

// Repository claims and completes outbox publication work.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a PostgreSQL outbox repository.
func NewRepository(pool *pgxpool.Pool) (*Repository, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: pool is required", ErrInvalidArgument)
	}

	return &Repository{pool: pool}, nil
}

// Claim leases a bounded batch using a fresh, repository-generated UUID token.
func (r *Repository) Claim(
	ctx context.Context,
	limit int,
	lease time.Duration,
) (claimed []outbox.ClaimedEvent, err error) {
	if limit <= 0 || limit > maxClaimBatch {
		return nil, fmt.Errorf("%w: claim limit must be between 1 and %d", ErrInvalidArgument, maxClaimBatch)
	}
	if lease < minClaimLease || lease > maxClaimLease || lease%time.Second != 0 {
		return nil, fmt.Errorf("%w: claim lease must be whole seconds between %s and %s", ErrInvalidArgument, minClaimLease, maxClaimLease)
	}
	claimToken, err := newClaimToken()
	if err != nil {
		return nil, err
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("beginning outbox claim transaction: %w", err)
	}
	defer finishTransaction(ctx, tx, &err)

	rows, err := tx.Query(ctx, claimEventsQuery, limit, claimToken, int64(lease/time.Second))
	if err != nil {
		return nil, fmt.Errorf("claiming outbox events: %w", err)
	}
	defer rows.Close()

	claimed = []outbox.ClaimedEvent{}
	for rows.Next() {
		stored, scanErr := scanClaimedEvent(rows, claimToken)
		if scanErr != nil {
			return nil, scanErr
		}
		claimed = append(claimed, stored)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating claimed outbox events: %w", err)
	}
	rows.Close()

	slices.SortFunc(claimed, func(left, right outbox.ClaimedEvent) int {
		return cmp.Compare(left.Sequence, right.Sequence)
	})
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing outbox claim transaction: %w", err)
	}

	return claimed, nil
}

// MarkPublished records broker acknowledgement for the current claim token.
func (r *Repository) MarkPublished(ctx context.Context, eventID, claimToken string) error {
	parsedEventID, parsedToken, err := parseClaimIdentity(eventID, claimToken)
	if err != nil {
		return err
	}

	commandTag, err := r.pool.Exec(ctx, markPublishedQuery, parsedEventID, parsedToken)
	if err != nil {
		return fmt.Errorf("marking outbox event published: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return ErrClaimLost
	}

	return nil
}

// MarkFailed releases the current claim and schedules a bounded publication retry.
func (r *Repository) MarkFailed(
	ctx context.Context,
	eventID, claimToken string,
	retryAfter time.Duration,
	diagnostic string,
) error {
	parsedEventID, parsedToken, err := parseClaimIdentity(eventID, claimToken)
	if err != nil {
		return err
	}
	if retryAfter < 0 || retryAfter > maxRetryDelay || retryAfter%time.Second != 0 {
		return fmt.Errorf("%w: retry delay must be whole seconds between 0s and %s", ErrInvalidArgument, maxRetryDelay)
	}
	diagnostic = sanitizeDiagnostic(diagnostic)

	commandTag, err := r.pool.Exec(
		ctx,
		markFailedQuery,
		parsedEventID,
		parsedToken,
		int64(retryAfter/time.Second),
		diagnostic,
	)
	if err != nil {
		return fmt.Errorf("marking outbox event failed: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return ErrClaimLost
	}

	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanClaimedEvent(row rowScanner, claimToken string) (outbox.ClaimedEvent, error) {
	stored := outbox.ClaimedEvent{ClaimToken: claimToken}
	var eventID string
	var params event.DraftParams
	var payload []byte
	if err := row.Scan(
		&eventID,
		&stored.Sequence,
		&params.EventType,
		&params.EventVersion,
		&params.AggregateType,
		&params.AggregateID,
		&params.AggregateVersion,
		&params.CorrelationID,
		&params.CausationID,
		&params.OccurredAt,
		&payload,
		&stored.PublishAttempts,
	); err != nil {
		return outbox.ClaimedEvent{}, fmt.Errorf("scanning claimed outbox event: %w", err)
	}
	params.Payload = payload
	draft, err := event.NewDraft(params)
	if err != nil {
		return outbox.ClaimedEvent{}, fmt.Errorf("%w: event draft: %w", ErrCorruptData, err)
	}
	stored.Envelope, err = event.NewEnvelope(eventID, draft)
	if err != nil {
		return outbox.ClaimedEvent{}, fmt.Errorf("%w: event envelope: %w", ErrCorruptData, err)
	}

	return stored, nil
}

func parseClaimIdentity(eventID, claimToken string) (pgtype.UUID, pgtype.UUID, error) {
	parsedEventID, err := parseUUID(eventID)
	if err != nil {
		return pgtype.UUID{}, pgtype.UUID{}, fmt.Errorf("%w: event ID: %w", ErrInvalidArgument, err)
	}
	parsedToken, err := parseUUID(claimToken)
	if err != nil {
		return pgtype.UUID{}, pgtype.UUID{}, fmt.Errorf("%w: claim token: %w", ErrInvalidArgument, err)
	}

	return parsedEventID, parsedToken, nil
}

func parseUUID(value string) (pgtype.UUID, error) {
	var parsed pgtype.UUID
	if err := parsed.Scan(value); err != nil {
		return pgtype.UUID{}, fmt.Errorf("parsing UUID: %w", err)
	}
	if !parsed.Valid {
		return pgtype.UUID{}, errors.New("parsing UUID: value is empty")
	}

	return parsed, nil
}

func newClaimToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generating outbox claim token: %w", err)
	}
	token[6] = token[6]&0x0f | 0x40
	token[8] = token[8]&0x3f | 0x80

	return fmt.Sprintf(
		"%x-%x-%x-%x-%x",
		token[0:4],
		token[4:6],
		token[6:8],
		token[8:10],
		token[10:16],
	), nil
}

func sanitizeDiagnostic(value string) string {
	sanitized := make([]byte, 0, min(len(value), maxDiagnosticBytes))
	for _, character := range value {
		if len(sanitized) == maxDiagnosticBytes {
			break
		}
		if character < ' ' || character > '~' {
			sanitized = append(sanitized, '?')
			continue
		}
		sanitized = append(sanitized, byte(character))
	}
	if len(sanitized) == 0 {
		return fallbackDiagnostic
	}

	return string(sanitized)
}

func finishTransaction(ctx context.Context, tx pgx.Tx, resultErr *error) {
	rollbackErr := tx.Rollback(ctx)
	if rollbackErr == nil || errors.Is(rollbackErr, pgx.ErrTxClosed) {
		return
	}

	*resultErr = errors.Join(*resultErr, fmt.Errorf("rolling back outbox transaction: %w", rollbackErr))
}
