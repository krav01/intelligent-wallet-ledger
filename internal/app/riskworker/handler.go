// Package riskworker coordinates deterministic transfer risk assessment.
package riskworker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	inboxpostgres "github.com/krav01/intelligent-wallet-ledger/internal/inbox/postgres"
	outboxpostgres "github.com/krav01/intelligent-wallet-ledger/internal/outbox/postgres"
	riskdomain "github.com/krav01/intelligent-wallet-ledger/internal/risk/domain"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
	transferevents "github.com/krav01/intelligent-wallet-ledger/internal/transfer/events"
	transferpostgres "github.com/krav01/intelligent-wallet-ledger/internal/transfer/postgres"
)

const consumerName = "risk-worker.v1"

var (
	// ErrInvalidArgument indicates invalid risk-worker configuration or decision data.
	ErrInvalidArgument = errors.New("risk worker: invalid argument")
	// ErrUnknownPolicy indicates an event references an unavailable policy version.
	ErrUnknownPolicy = errors.New("risk worker: unknown policy version")
)

type Handler struct {
	pool   *pgxpool.Pool
	policy riskdomain.Policy
	now    func() time.Time
}

func NewHandler(pool *pgxpool.Pool, policy riskdomain.Policy, now func() time.Time) (*Handler, error) {
	if pool == nil || policy.Version() == "" || now == nil {
		return nil, ErrInvalidArgument
	}
	return &Handler{pool: pool, policy: policy, now: now}, nil
}

func (h *Handler) Handle(ctx context.Context, envelope event.Envelope) error {
	requested, err := transferevents.ParseRequested(envelope)
	if err != nil {
		return err
	}
	if requested.RiskPolicyVersion() != h.policy.Version() {
		return fmt.Errorf("%w: %s", ErrUnknownPolicy, requested.RiskPolicyVersion())
	}
	tx, err := h.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("beginning risk transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	reserved, err := inboxpostgres.ReserveTx(ctx, tx, consumerName, envelope)
	if err != nil {
		return err
	}
	if !reserved {
		return tx.Commit(ctx)
	}
	current, err := transferpostgres.LoadLifecycleForUpdateTx(ctx, tx, requested.Transfer().ID())
	if err != nil {
		return err
	}
	if current.Status() != transferdomain.StatusPendingRisk ||
		current.Transfer().ID() != requested.Transfer().ID() ||
		current.Transfer().RequesterID() != requested.Transfer().RequesterID() ||
		current.Transfer().SourceAccountID() != requested.Transfer().SourceAccountID() ||
		current.Transfer().DestinationAccountID() != requested.Transfer().DestinationAccountID() ||
		current.Transfer().Amount() != requested.Transfer().Amount() ||
		!current.Transfer().RequestedAt().Equal(requested.Transfer().RequestedAt()) ||
		current.RiskPolicyVersion() != requested.RiskPolicyVersion() {
		return transferpostgres.ErrStateConflict
	}
	evaluation, err := h.policy.Evaluate(riskdomain.Input{Amount: current.Transfer().Amount()})
	if err != nil {
		return err
	}
	next, err := transition(current, evaluation.Decision())
	if err != nil {
		return err
	}
	assessedAt := h.now().UTC()
	if err := transferpostgres.StoreRiskAssessmentTx(ctx, tx, current, next, evaluation, envelope.EventID(), assessedAt); err != nil {
		return err
	}
	draft, err := transferevents.RiskAssessed(next, evaluation, assessedAt, envelope.EventID())
	if err != nil {
		return err
	}
	if _, err := outboxpostgres.AddTx(ctx, tx, draft); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func transition(lifecycle transferdomain.Lifecycle, decision riskdomain.Decision) (transferdomain.Lifecycle, error) {
	switch decision {
	case riskdomain.DecisionApprove:
		return lifecycle.Approve()
	case riskdomain.DecisionReview:
		return lifecycle.RequireReview()
	case riskdomain.DecisionDecline:
		return lifecycle.Decline()
	default:
		return transferdomain.Lifecycle{}, ErrInvalidArgument
	}
}
