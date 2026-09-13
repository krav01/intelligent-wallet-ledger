// Package transactionworker posts risk-approved transfers exactly once.
package transactionworker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	inboxpostgres "github.com/krav01/intelligent-wallet-ledger/internal/inbox/postgres"
	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	ledgerpostgres "github.com/krav01/intelligent-wallet-ledger/internal/ledger/postgres"
	outboxpostgres "github.com/krav01/intelligent-wallet-ledger/internal/outbox/postgres"
	riskdomain "github.com/krav01/intelligent-wallet-ledger/internal/risk/domain"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
	transferevents "github.com/krav01/intelligent-wallet-ledger/internal/transfer/events"
	transferpostgres "github.com/krav01/intelligent-wallet-ledger/internal/transfer/postgres"
)

const (
	consumerName            = "transaction-worker.v1"
	insufficientFundsReason = "insufficient_funds"
)

// ErrInvalidArgument indicates invalid transaction-worker construction or event data.
var ErrInvalidArgument = errors.New("transaction worker: invalid argument")

// Handler posts approved transfers and persists their terminal lifecycle in one transaction.
type Handler struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

// NewHandler creates a transaction posting handler with the supplied clock.
func NewHandler(pool *pgxpool.Pool, now func() time.Time) (*Handler, error) {
	if pool == nil || now == nil {
		return nil, ErrInvalidArgument
	}
	return &Handler{pool: pool, now: now}, nil
}

// Handle consumes one risk assessment. Approved transfers are posted; all other valid decisions are recorded as consumed.
func (h *Handler) Handle(ctx context.Context, envelope event.Envelope) error {
	if envelope.EventType() == transferevents.ReviewDecidedType {
		return h.handleReviewDecision(ctx, envelope)
	}
	assessment, err := transferevents.ParseRiskAssessed(envelope)
	if err != nil {
		return err
	}
	tx, err := h.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("beginning transaction posting: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	reserved, err := inboxpostgres.ReserveTx(ctx, tx, consumerName, envelope)
	if err != nil {
		return err
	}
	if !reserved {
		return tx.Commit(ctx)
	}
	current, err := transferpostgres.LoadLifecycleForUpdateTx(ctx, tx, assessment.TransferID())
	if err != nil {
		return err
	}
	if current.Version() != envelope.AggregateVersion() ||
		current.RiskPolicyVersion() != assessment.RiskPolicyVersion() ||
		current.Status() != statusForDecision(assessment.Decision()) {
		return transferpostgres.ErrStateConflict
	}
	if assessment.Decision() != riskdomain.DecisionApprove {
		return tx.Commit(ctx)
	}
	return h.post(ctx, tx, current, envelope.EventID(), h.now().UTC())
}

func (h *Handler) handleReviewDecision(ctx context.Context, envelope event.Envelope) error {
	decision, err := transferevents.ParseReviewDecided(envelope)
	if err != nil {
		return err
	}
	tx, err := h.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("beginning transaction posting: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	reserved, err := inboxpostgres.ReserveTx(ctx, tx, consumerName, envelope)
	if err != nil {
		return err
	}
	if !reserved {
		return tx.Commit(ctx)
	}
	current, err := transferpostgres.LoadLifecycleForUpdateTx(ctx, tx, decision.TransferID())
	if err != nil {
		return err
	}
	if current.Version() != envelope.AggregateVersion() || current.Status().String() != decision.Decision() {
		return transferpostgres.ErrStateConflict
	}
	if decision.Decision() == "declined" {
		return tx.Commit(ctx)
	}
	return h.post(ctx, tx, current, envelope.EventID(), h.now().UTC())
}

func (h *Handler) post(ctx context.Context, tx pgx.Tx, current transferdomain.Lifecycle, causationID string, postedAt time.Time) error {
	entry, err := journalEntry(current, postedAt)
	if err != nil {
		return err
	}
	if err := ledgerpostgres.PostTx(ctx, tx, entry); err != nil {
		if !errors.Is(err, ledgerpostgres.ErrInsufficientFunds) {
			return err
		}
		return h.fail(ctx, tx, current, causationID, postedAt)
	}
	next, err := current.Complete()
	if err != nil {
		return err
	}
	if err := transferpostgres.StoreLifecycleTransitionTx(ctx, tx, current, next); err != nil {
		return err
	}
	draft, err := transferevents.CompletedLifecycle(next, postedAt, causationID)
	if err != nil {
		return err
	}
	if _, err := outboxpostgres.AddTx(ctx, tx, draft); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (h *Handler) fail(
	ctx context.Context,
	tx pgx.Tx,
	current transferdomain.Lifecycle,
	causationID string,
	failedAt time.Time,
) error {
	next, err := current.Fail(insufficientFundsReason)
	if err != nil {
		return err
	}
	if err := transferpostgres.StoreLifecycleTransitionTx(ctx, tx, current, next); err != nil {
		return err
	}
	draft, err := transferevents.Failed(next, failedAt, causationID)
	if err != nil {
		return err
	}
	if _, err := outboxpostgres.AddTx(ctx, tx, draft); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func journalEntry(lifecycle transferdomain.Lifecycle, recordedAt time.Time) (ledgerdomain.JournalEntry, error) {
	if lifecycle.Status() != transferdomain.StatusApproved {
		return ledgerdomain.JournalEntry{}, ErrInvalidArgument
	}
	amount := lifecycle.Transfer().Amount()
	debit, err := amount.Negate()
	if err != nil {
		return ledgerdomain.JournalEntry{}, fmt.Errorf("negating transfer amount: %w", err)
	}
	sourcePosting, err := ledgerdomain.NewPosting(lifecycle.Transfer().SourceAccountID(), debit)
	if err != nil {
		return ledgerdomain.JournalEntry{}, fmt.Errorf("creating source posting: %w", err)
	}
	destinationPosting, err := ledgerdomain.NewPosting(lifecycle.Transfer().DestinationAccountID(), amount)
	if err != nil {
		return ledgerdomain.JournalEntry{}, fmt.Errorf("creating destination posting: %w", err)
	}
	entry, err := ledgerdomain.NewJournalEntry(ledgerdomain.NewJournalEntryParams{
		ID:         lifecycle.Transfer().ID(),
		Postings:   []ledgerdomain.Posting{sourcePosting, destinationPosting},
		RecordedAt: recordedAt,
	})
	if err != nil {
		return ledgerdomain.JournalEntry{}, fmt.Errorf("creating transfer journal entry: %w", err)
	}
	return entry, nil
}

func statusForDecision(decision riskdomain.Decision) transferdomain.Status {
	switch decision {
	case riskdomain.DecisionApprove:
		return transferdomain.StatusApproved
	case riskdomain.DecisionReview:
		return transferdomain.StatusReviewRequired
	case riskdomain.DecisionDecline:
		return transferdomain.StatusDeclined
	default:
		return transferdomain.StatusUnknown
	}
}
