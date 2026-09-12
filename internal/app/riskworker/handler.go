// Package riskworker coordinates deterministic transfer risk assessment.
package riskworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

// VelocityObserver captures one transfer in a source-account velocity window.
type VelocityObserver interface {
	Observe(context.Context, string, string, time.Time) (int, error)
}

// Handler assesses pending transfers exactly once per consumed outbox event.
type Handler struct {
	pool     *pgxpool.Pool
	policies map[string]riskdomain.Policy
	velocity VelocityObserver
	logger   *slog.Logger
	now      func() time.Time
}

// NewHandler creates a risk assessment handler with the supplied policy and clock.
func NewHandler(pool *pgxpool.Pool, policy riskdomain.Policy, now func() time.Time) (*Handler, error) {
	return NewHandlerWithPolicies(pool, []riskdomain.Policy{policy}, now)
}

// NewHandlerWithPolicies creates a handler that selects an immutable policy by version.
func NewHandlerWithPolicies(pool *pgxpool.Pool, policies []riskdomain.Policy, now func() time.Time) (*Handler, error) {
	return NewHandlerWithPoliciesAndVelocity(pool, policies, noopVelocityObserver{}, slog.Default(), now)
}

// NewHandlerWithPoliciesAndVelocity creates a handler with a captured velocity observer.
func NewHandlerWithPoliciesAndVelocity(
	pool *pgxpool.Pool,
	policies []riskdomain.Policy,
	velocity VelocityObserver,
	logger *slog.Logger,
	now func() time.Time,
) (*Handler, error) {
	if pool == nil || len(policies) == 0 || velocity == nil || now == nil {
		return nil, ErrInvalidArgument
	}
	if logger == nil {
		logger = slog.Default()
	}
	byVersion := make(map[string]riskdomain.Policy, len(policies))
	for _, policy := range policies {
		if policy.Version() == "" {
			return nil, ErrInvalidArgument
		}
		if _, exists := byVersion[policy.Version()]; exists {
			return nil, ErrInvalidArgument
		}
		byVersion[policy.Version()] = policy
	}
	return &Handler{pool: pool, policies: byVersion, velocity: velocity, logger: logger, now: now}, nil
}

// Handle persists an assessment and its resulting event in one transaction.
func (h *Handler) Handle(ctx context.Context, envelope event.Envelope) error {
	requested, err := transferevents.ParseRequested(envelope)
	if err != nil {
		return err
	}
	policy, exists := h.policies[requested.RiskPolicyVersion()]
	if !exists {
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
	assessedAt := h.now().UTC()
	input := h.captureInput(ctx, current, assessedAt)
	evaluation, err := policy.Evaluate(input)
	if err != nil {
		return err
	}
	next, err := transition(current, evaluation.Decision())
	if err != nil {
		return err
	}
	if err := transferpostgres.StoreRiskAssessmentTx(ctx, tx, current, next, input, evaluation, envelope.EventID(), assessedAt); err != nil {
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

func (h *Handler) captureInput(ctx context.Context, current transferdomain.Lifecycle, assessedAt time.Time) riskdomain.Input {
	input := riskdomain.Input{Amount: current.Transfer().Amount()}
	count, err := h.velocity.Observe(ctx, current.Transfer().SourceAccountID(), current.Transfer().ID(), assessedAt)
	if err != nil {
		h.logger.Warn("velocity observation degraded", "transfer_id", current.Transfer().ID(), "error", err)
		input.VelocityDegraded = true
		return input
	}
	input.VelocityTransferCount = count
	return input
}

type noopVelocityObserver struct{}

func (noopVelocityObserver) Observe(context.Context, string, string, time.Time) (int, error) {
	return 0, nil
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
