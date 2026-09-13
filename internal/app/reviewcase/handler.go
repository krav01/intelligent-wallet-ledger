// Package reviewcase authorizes and records analyst decisions for open review cases.
package reviewcase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	outboxpostgres "github.com/krav01/intelligent-wallet-ledger/internal/outbox/postgres"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
	transferevents "github.com/krav01/intelligent-wallet-ledger/internal/transfer/events"
	transferpostgres "github.com/krav01/intelligent-wallet-ledger/internal/transfer/postgres"
)

var (
	// ErrInvalidArgument indicates malformed review-decision input or construction.
	ErrInvalidArgument = errors.New("review case: invalid argument")
	// ErrUnauthorized indicates a principal without the analyst role.
	ErrUnauthorized = errors.New("review case: principal is not an analyst")
)

// Role identifies a trusted principal's authorization role.
type Role string

// RoleAnalyst permits an analyst to resolve review cases.
const RoleAnalyst Role = "analyst"

// TrustedPrincipal is constructed only by an authenticated transport adapter.
type TrustedPrincipal struct {
	Subject string
	Role    Role
}

// Decision is an allowed analyst outcome for an open review case.
type Decision string

const (
	// DecisionApprove authorizes normal financial posting.
	DecisionApprove Decision = "approved"
	// DecisionDecline terminates the transfer without posting.
	DecisionDecline Decision = "declined"
)

// Handler records authorized review decisions.
type Handler struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

// NewHandler creates a review decision handler.
func NewHandler(pool *pgxpool.Pool, now func() time.Time) (*Handler, error) {
	if pool == nil || now == nil {
		return nil, ErrInvalidArgument
	}
	return &Handler{pool: pool, now: now}, nil
}

// Decide atomically persists one authorized analyst decision.
func (h *Handler) Decide(ctx context.Context, principal TrustedPrincipal, transferID string, decision Decision) error {
	if principal.Role != RoleAnalyst || !validSubject(principal.Subject) {
		return ErrUnauthorized
	}
	if decision != DecisionApprove && decision != DecisionDecline {
		return ErrInvalidArgument
	}
	tx, err := h.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("beginning review decision: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	current, err := transferpostgres.LoadLifecycleForUpdateTx(ctx, tx, transferID)
	if err != nil {
		return err
	}
	if current.Status() != transferdomain.StatusReviewRequired {
		return transferpostgres.ErrStateConflict
	}
	var next transferdomain.Lifecycle
	if decision == DecisionApprove {
		next, err = current.Approve()
	} else {
		next, err = current.Decline()
	}
	if err != nil {
		return err
	}
	decidedAt := h.now().UTC()
	if err := transferpostgres.StoreLifecycleTransitionTx(ctx, tx, current, next); err != nil {
		return err
	}
	if err := transferpostgres.DecideReviewCaseTx(ctx, tx, transferID, string(decision), principal.Subject, decidedAt); err != nil {
		return err
	}
	draft, err := transferevents.ReviewDecided(next, principal.Subject, decidedAt)
	if err != nil {
		return err
	}
	if _, err := outboxpostgres.AddTx(ctx, tx, draft); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func validSubject(value string) bool {
	value = strings.TrimSpace(value)
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
