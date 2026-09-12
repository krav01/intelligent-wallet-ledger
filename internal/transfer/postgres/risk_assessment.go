package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	riskdomain "github.com/krav01/intelligent-wallet-ledger/internal/risk/domain"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
)

// ErrStateConflict indicates a lifecycle changed before its expected transition committed.
var ErrStateConflict = errors.New("transfer repository: lifecycle state conflict")

const (
	selectLifecycleForUpdateQuery = `
SELECT
    id::text,
    requester_id::text,
    idempotency_key,
    source_account_id::text,
    destination_account_id::text,
    currency,
    amount_minor,
    requested_at,
    status,
    state_version,
    risk_policy_version,
    journal_entry_id::text,
    failure_reason
FROM transfers
WHERE id = $1
FOR UPDATE`

	updateLifecycleQuery = `
UPDATE transfers
SET status = $2,
    state_version = $3
WHERE id = $1
  AND status = $4
  AND state_version = $5`

	insertRiskAssessmentQuery = `
INSERT INTO transfer_risk_assessments (
    transfer_id,
    lifecycle_version,
    causation_event_id,
    policy_version,
    captured_input,
    score,
    decision,
    signals,
    assessed_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	insertReviewCaseQuery = `
INSERT INTO transfer_review_cases (
    transfer_id,
    assessment_lifecycle_version,
    opened_at
)
VALUES ($1, $2, $3)`
)

// LoadLifecycleForUpdateTx returns one durable lifecycle while holding its row lock.
// The caller owns the transaction lifecycle and must commit or roll back the lock.
func LoadLifecycleForUpdateTx(
	ctx context.Context,
	tx pgx.Tx,
	transferID string,
) (transferdomain.Lifecycle, error) {
	if tx == nil {
		return transferdomain.Lifecycle{}, fmt.Errorf("%w: transaction is required", ErrInvalidArgument)
	}
	if _, err := parseUUID(transferID); err != nil {
		return transferdomain.Lifecycle{}, fmt.Errorf("%w: transfer ID: %w", ErrInvalidArgument, err)
	}

	var record lifecycleRecord
	err := tx.QueryRow(ctx, selectLifecycleForUpdateQuery, transferID).Scan(
		&record.id,
		&record.requesterID,
		&record.idempotencyKey,
		&record.sourceAccountID,
		&record.destinationAccountID,
		&record.currencyCode,
		&record.amountMinor,
		&record.requestedAt,
		&record.status,
		&record.version,
		&record.riskPolicyVersion,
		&record.journalEntryID,
		&record.failureReason,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return transferdomain.Lifecycle{}, ErrNotFound
	}
	if err != nil {
		return transferdomain.Lifecycle{}, fmt.Errorf("loading transfer lifecycle: %w", err)
	}

	lifecycle, err := record.lifecycle()
	if err != nil {
		return transferdomain.Lifecycle{}, err
	}
	return lifecycle, nil
}

// StoreRiskAssessmentTx atomically advances a pending lifecycle and appends its risk assessment.
// The caller must reserve the consumed event and write its result event in the same transaction.
func StoreRiskAssessmentTx(
	ctx context.Context,
	tx pgx.Tx,
	previous transferdomain.Lifecycle,
	next transferdomain.Lifecycle,
	input riskdomain.Input,
	evaluation riskdomain.Evaluation,
	causationEventID string,
	assessedAt time.Time,
) error {
	if tx == nil {
		return fmt.Errorf("%w: transaction is required", ErrInvalidArgument)
	}
	encodedInput, signals, causationID, err := prepareRiskAssessment(previous, next, input, evaluation, causationEventID, assessedAt)
	if err != nil {
		return err
	}

	commandTag, err := tx.Exec(
		ctx,
		updateLifecycleQuery,
		previous.Transfer().ID(),
		next.Status().String(),
		next.Version(),
		previous.Status().String(),
		previous.Version(),
	)
	if err != nil {
		return fmt.Errorf("updating transfer lifecycle: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return ErrStateConflict
	}

	if _, err := tx.Exec(
		ctx,
		insertRiskAssessmentQuery,
		previous.Transfer().ID(),
		next.Version(),
		causationID,
		evaluation.PolicyVersion(),
		encodedInput,
		evaluation.Score(),
		evaluation.Decision().String(),
		signals,
		assessedAt.UTC(),
	); err != nil {
		return fmt.Errorf("storing risk assessment: %w", err)
	}
	if next.Status() == transferdomain.StatusReviewRequired {
		if _, err := tx.Exec(
			ctx,
			insertReviewCaseQuery,
			previous.Transfer().ID(),
			next.Version(),
			assessedAt.UTC(),
		); err != nil {
			return fmt.Errorf("storing transfer review case: %w", err)
		}
	}

	return nil
}

// StoreLifecycleTransitionTx persists a non-risk lifecycle transition under the held row lock.
func StoreLifecycleTransitionTx(ctx context.Context, tx pgx.Tx, previous, next transferdomain.Lifecycle) error {
	if tx == nil {
		return fmt.Errorf("%w: transaction is required", ErrInvalidArgument)
	}
	commandTag, err := tx.Exec(
		ctx,
		`UPDATE transfers
SET status = $2,
    state_version = $3,
    journal_entry_id = NULLIF($4, '')::uuid,
    failure_reason = NULLIF($5, '')
WHERE id = $1
  AND status = $6
  AND state_version = $7`,
		previous.Transfer().ID(),
		next.Status().String(),
		next.Version(),
		next.JournalEntryID(),
		next.FailureReason(),
		previous.Status().String(),
		previous.Version(),
	)
	if err != nil {
		return fmt.Errorf("updating transfer lifecycle: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return ErrStateConflict
	}
	return nil
}

type lifecycleRecord struct {
	id                   string
	requesterID          string
	idempotencyKey       string
	sourceAccountID      string
	destinationAccountID string
	currencyCode         string
	amountMinor          int64
	requestedAt          time.Time
	status               string
	version              int64
	riskPolicyVersion    *string
	journalEntryID       *string
	failureReason        *string
}

func (r lifecycleRecord) lifecycle() (transferdomain.Lifecycle, error) {
	currency, err := ledgerdomain.ParseCurrency(r.currencyCode)
	if err != nil {
		return transferdomain.Lifecycle{}, fmt.Errorf("%w: transfer currency: %w", ErrCorruptData, err)
	}
	amount, err := ledgerdomain.NewMoney(r.amountMinor, currency)
	if err != nil {
		return transferdomain.Lifecycle{}, fmt.Errorf("%w: transfer amount: %w", ErrCorruptData, err)
	}
	transfer, err := transferdomain.NewTransfer(transferdomain.NewTransferParams{
		ID:                   r.id,
		IdempotencyKey:       r.idempotencyKey,
		RequesterID:          r.requesterID,
		SourceAccountID:      r.sourceAccountID,
		DestinationAccountID: r.destinationAccountID,
		Amount:               amount,
		RequestedAt:          r.requestedAt,
	})
	if err != nil {
		return transferdomain.Lifecycle{}, fmt.Errorf("%w: transfer: %w", ErrCorruptData, err)
	}
	status, ok := lifecycleStatus(r.status)
	if !ok {
		return transferdomain.Lifecycle{}, fmt.Errorf("%w: lifecycle status", ErrCorruptData)
	}
	lifecycle, err := transferdomain.NewLifecycle(transferdomain.LifecycleParams{
		Transfer:          transfer,
		Status:            status,
		Version:           r.version,
		RiskPolicyVersion: nullableValue(r.riskPolicyVersion),
		JournalEntryID:    nullableValue(r.journalEntryID),
		FailureReason:     nullableValue(r.failureReason),
	})
	if err != nil {
		return transferdomain.Lifecycle{}, fmt.Errorf("%w: lifecycle: %w", ErrCorruptData, err)
	}
	return lifecycle, nil
}

func prepareRiskAssessment(
	previous transferdomain.Lifecycle,
	next transferdomain.Lifecycle,
	input riskdomain.Input,
	evaluation riskdomain.Evaluation,
	causationEventID string,
	assessedAt time.Time,
) (json.RawMessage, json.RawMessage, pgtype.UUID, error) {
	causationID, err := parseUUID(causationEventID)
	if err != nil {
		return nil, nil, pgtype.UUID{}, fmt.Errorf("%w: causation event ID: %w", ErrInvalidArgument, err)
	}
	if assessedAt.IsZero() || previous.Status() != transferdomain.StatusPendingRisk ||
		previous.Transfer().ID() != next.Transfer().ID() ||
		input.Amount != previous.Transfer().Amount() || input.VelocityTransferCount < 0 ||
		previous.RiskPolicyVersion() != next.RiskPolicyVersion() ||
		next.Version() != previous.Version()+1 ||
		evaluation.PolicyVersion() != previous.RiskPolicyVersion() ||
		!matchesRiskDecision(next.Status(), evaluation.Decision()) {
		return nil, nil, pgtype.UUID{}, fmt.Errorf("%w: risk assessment transition", ErrInvalidArgument)
	}

	encodedInput, err := json.Marshal(struct {
		AmountMinor           int64  `json:"amount_minor"`
		Currency              string `json:"currency"`
		VelocityTransferCount int    `json:"velocity_transfer_count"`
		VelocityDegraded      bool   `json:"velocity_degraded"`
	}{
		AmountMinor:           input.Amount.MinorUnits(),
		Currency:              input.Amount.Currency().String(),
		VelocityTransferCount: input.VelocityTransferCount,
		VelocityDegraded:      input.VelocityDegraded,
	})
	if err != nil {
		return nil, nil, pgtype.UUID{}, fmt.Errorf("encoding risk assessment input: %w", err)
	}
	signals := evaluation.Signals()
	encodedSignals := make([]struct {
		Code         string `json:"code"`
		Contribution int    `json:"contribution"`
	}, len(signals))
	for index, signal := range signals {
		encodedSignals[index] = struct {
			Code         string `json:"code"`
			Contribution int    `json:"contribution"`
		}{
			Code:         string(signal.Code()),
			Contribution: signal.Contribution(),
		}
	}
	encodedSignalsJSON, err := json.Marshal(encodedSignals)
	if err != nil {
		return nil, nil, pgtype.UUID{}, fmt.Errorf("encoding risk assessment signals: %w", err)
	}

	return encodedInput, encodedSignalsJSON, causationID, nil
}

func lifecycleStatus(value string) (transferdomain.Status, bool) {
	switch value {
	case transferdomain.StatusPendingRisk.String():
		return transferdomain.StatusPendingRisk, true
	case transferdomain.StatusApproved.String():
		return transferdomain.StatusApproved, true
	case transferdomain.StatusReviewRequired.String():
		return transferdomain.StatusReviewRequired, true
	case transferdomain.StatusDeclined.String():
		return transferdomain.StatusDeclined, true
	case transferdomain.StatusCompleted.String():
		return transferdomain.StatusCompleted, true
	case transferdomain.StatusFailed.String():
		return transferdomain.StatusFailed, true
	default:
		return transferdomain.StatusUnknown, false
	}
}

func matchesRiskDecision(status transferdomain.Status, decision riskdomain.Decision) bool {
	switch decision {
	case riskdomain.DecisionApprove:
		return status == transferdomain.StatusApproved
	case riskdomain.DecisionReview:
		return status == transferdomain.StatusReviewRequired
	case riskdomain.DecisionDecline:
		return status == transferdomain.StatusDeclined
	default:
		return false
	}
}

func nullableValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
