// Package postgres persists audit records in caller-owned PostgreSQL transactions.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var ErrInvalidArgument = errors.New("audit repository: invalid argument")

const insertReviewDecisionQuery = `
INSERT INTO transfer_review_audit_records (
    transfer_id,
    lifecycle_version,
    decision,
    actor_subject,
    occurred_at
)
VALUES ($1, $2, $3, $4, $5)`

// ReviewDecision is the complete audit context for one analyst decision.
type ReviewDecision struct {
	TransferID       string
	LifecycleVersion int64
	Decision         string
	ActorSubject     string
	OccurredAt       time.Time
}

// AppendReviewDecisionTx writes one review-decision audit record in a caller-owned transaction.
func AppendReviewDecisionTx(ctx context.Context, tx pgx.Tx, record ReviewDecision) error {
	if tx == nil {
		return fmt.Errorf("%w: transaction is required", ErrInvalidArgument)
	}
	if err := validateReviewDecision(record); err != nil {
		return err
	}
	if _, err := tx.Exec(
		ctx,
		insertReviewDecisionQuery,
		record.TransferID,
		record.LifecycleVersion,
		record.Decision,
		record.ActorSubject,
		record.OccurredAt.UTC(),
	); err != nil {
		return fmt.Errorf("inserting transfer review audit record: %w", err)
	}
	return nil
}

func validateReviewDecision(record ReviewDecision) error {
	var transferID pgtype.UUID
	if err := transferID.Scan(record.TransferID); err != nil {
		return fmt.Errorf("%w: transfer ID: %w", ErrInvalidArgument, err)
	}
	if record.LifecycleVersion < 3 {
		return fmt.Errorf("%w: lifecycle version", ErrInvalidArgument)
	}
	if record.Decision != "approved" && record.Decision != "declined" {
		return fmt.Errorf("%w: decision", ErrInvalidArgument)
	}
	if !validSubject(record.ActorSubject) {
		return fmt.Errorf("%w: actor subject", ErrInvalidArgument)
	}
	if record.OccurredAt.IsZero() {
		return fmt.Errorf("%w: occurred at", ErrInvalidArgument)
	}
	return nil
}

func validSubject(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if r < '!' || r > '~' {
			return false
		}
	}
	return true
}
