//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	auditpostgres "github.com/krav01/intelligent-wallet-ledger/internal/audit/postgres"
	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	ledgerpostgres "github.com/krav01/intelligent-wallet-ledger/internal/ledger/postgres"
	outboxpostgres "github.com/krav01/intelligent-wallet-ledger/internal/outbox/postgres"
	riskdomain "github.com/krav01/intelligent-wallet-ledger/internal/risk/domain"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
	transferevents "github.com/krav01/intelligent-wallet-ledger/internal/transfer/events"
	transferpostgres "github.com/krav01/intelligent-wallet-ledger/internal/transfer/postgres"
)

func TestRepositoryCreateReplayAndConflict(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	ownerID := fixture.newUUID(t)
	sourceID := fixture.createAccount(t, ownerID, "USD", "customer", "active")
	destinationID := fixture.createAccount(t, fixture.newUUID(t), "USD", "customer", "active")
	fixture.fund(t, sourceID, 100)

	original := fixture.transfer(t, ownerID, sourceID, destinationID, "request-1", 60)
	created, err := repository.Create(t.Context(), original)
	if err != nil {
		t.Fatalf("Repository.Create() error = %v", err)
	}
	assertTransferEqual(t, created, original)
	fixture.assertCompletedEvent(t, original)

	replay := fixture.transfer(t, ownerID, sourceID, destinationID, "request-1", 60)
	replayed, err := repository.Create(t.Context(), replay)
	if err != nil {
		t.Fatalf("Repository.Create(replay) error = %v", err)
	}
	assertTransferEqual(t, replayed, original)
	fixture.assertBalance(t, sourceID, 40, 2)
	fixture.assertBalance(t, destinationID, 60, 1)
	fixture.assertTransferCount(t, ownerID, "request-1", 1)
	fixture.assertOutboxEventCount(t, original.ID(), 1)
	if _, err := fixture.pool.Exec(
		t.Context(),
		`UPDATE accounts SET status = 'frozen' WHERE id = $1`,
		sourceID,
	); err != nil {
		t.Fatalf("freezing source account: %v", err)
	}
	replayedAfterFreeze, err := repository.Create(t.Context(), replay)
	if err != nil {
		t.Fatalf("Repository.Create(replay after freeze) error = %v", err)
	}
	assertTransferEqual(t, replayedAfterFreeze, original)

	conflict := fixture.transfer(t, ownerID, sourceID, destinationID, "request-1", 61)
	if _, err := repository.Create(t.Context(), conflict); !errors.Is(err, transferpostgres.ErrIdempotencyConflict) {
		t.Fatalf("Repository.Create(conflict) error = %v, want ErrIdempotencyConflict", err)
	}
	fixture.assertBalance(t, sourceID, 40, 2)
	fixture.assertBalance(t, destinationID, 60, 1)
	fixture.assertOutboxEventCount(t, original.ID(), 1)
}

func TestRepositoryCreateStoresCompletedLifecycle(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	ownerID := fixture.newUUID(t)
	sourceID := fixture.createAccount(t, ownerID, "USD", "customer", "active")
	destinationID := fixture.createAccount(t, fixture.newUUID(t), "USD", "customer", "active")
	fixture.fund(t, sourceID, 100)

	transfer := fixture.transfer(t, ownerID, sourceID, destinationID, "completed-lifecycle", 40)
	if _, err := repository.Create(t.Context(), transfer); err != nil {
		t.Fatalf("Repository.Create() error = %v", err)
	}

	var status, journalEntryID string
	var stateVersion int64
	var riskPolicyVersion, failureReason *string
	if err := fixture.pool.QueryRow(
		t.Context(),
		`SELECT status, state_version, risk_policy_version, journal_entry_id::text, failure_reason
		 FROM transfers
		 WHERE id = $1`,
		transfer.ID(),
	).Scan(&status, &stateVersion, &riskPolicyVersion, &journalEntryID, &failureReason); err != nil {
		t.Fatalf("selecting transfer lifecycle: %v", err)
	}
	if status != "completed" || stateVersion != 1 || journalEntryID != transfer.ID() ||
		riskPolicyVersion != nil || failureReason != nil {
		t.Errorf(
			"lifecycle = (%q, %d, %v, %q, %v), want completed legacy lifecycle",
			status,
			stateVersion,
			riskPolicyVersion,
			journalEntryID,
			failureReason,
		)
	}
}

func TestTransferLifecycleConstraints(t *testing.T) {
	fixture := newFixture(t)
	ownerID := fixture.newUUID(t)
	sourceID := fixture.createAccount(t, ownerID, "USD", "customer", "active")
	destinationID := fixture.createAccount(t, fixture.newUUID(t), "USD", "customer", "active")

	pending := fixture.transfer(t, ownerID, sourceID, destinationID, "pending-lifecycle", 40)
	if _, err := fixture.pool.Exec(
		t.Context(),
		`INSERT INTO transfers (
			id, requester_id, idempotency_key, source_account_id, destination_account_id,
			currency, amount_minor, requested_at, status, state_version, risk_policy_version
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending_risk', 1, 'risk-v1')`,
		pending.ID(),
		pending.RequesterID(),
		pending.IdempotencyKey(),
		pending.SourceAccountID(),
		pending.DestinationAccountID(),
		pending.Amount().Currency().String(),
		pending.Amount().MinorUnits(),
		pending.RequestedAt(),
	); err != nil {
		t.Fatalf("inserting pending lifecycle transfer: %v", err)
	}

	invalidCompleted := fixture.transfer(t, ownerID, sourceID, destinationID, "invalid-completed", 41)
	if _, err := fixture.pool.Exec(
		t.Context(),
		`INSERT INTO transfers (
			id, requester_id, idempotency_key, source_account_id, destination_account_id,
			currency, amount_minor, requested_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		invalidCompleted.ID(),
		invalidCompleted.RequesterID(),
		invalidCompleted.IdempotencyKey(),
		invalidCompleted.SourceAccountID(),
		invalidCompleted.DestinationAccountID(),
		invalidCompleted.Amount().Currency().String(),
		invalidCompleted.Amount().MinorUnits(),
		invalidCompleted.RequestedAt(),
	); err == nil {
		t.Error("inserting completed lifecycle transfer without journal entry succeeded")
	}
}

func TestRepositoryCreatePendingStoresRequestedEventWithoutLedgerEffect(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	ownerID := fixture.newUUID(t)
	sourceID := fixture.createAccount(t, ownerID, "USD", "customer", "active")
	destinationID := fixture.createAccount(t, fixture.newUUID(t), "USD", "customer", "active")
	pending := fixture.transfer(t, ownerID, sourceID, destinationID, "pending-request", 60)
	lifecycle, err := transferdomain.NewPendingLifecycle(pending, "risk-v1")
	if err != nil {
		t.Fatalf("NewPendingLifecycle() error = %v", err)
	}

	created, err := repository.CreatePending(t.Context(), lifecycle)
	if err != nil {
		t.Fatalf("Repository.CreatePending() error = %v", err)
	}
	if created.Status() != transferdomain.StatusPendingRisk || created.Version() != 1 ||
		created.RiskPolicyVersion() != "risk-v1" {
		t.Errorf("CreatePending() lifecycle = (%s, %d, %q), want pending risk v1", created.Status(), created.Version(), created.RiskPolicyVersion())
	}
	fixture.assertBalance(t, sourceID, 0, 0)
	fixture.assertBalance(t, destinationID, 0, 0)

	var eventType, policyVersion string
	var aggregateVersion int64
	var payload []byte
	if err := fixture.pool.QueryRow(
		t.Context(),
		`SELECT event_type, aggregate_version, payload
		 FROM outbox_events
		 WHERE aggregate_type = 'transfer' AND aggregate_id = $1`,
		pending.ID(),
	).Scan(&eventType, &aggregateVersion, &payload); err != nil {
		t.Fatalf("selecting requested event: %v", err)
	}
	var eventPayload struct {
		RiskPolicyVersion string `json:"risk_policy_version"`
	}
	if err := json.Unmarshal(payload, &eventPayload); err != nil {
		t.Fatalf("decoding requested event payload: %v", err)
	}
	policyVersion = eventPayload.RiskPolicyVersion
	if eventType != transferevents.RequestedType || aggregateVersion != 1 || policyVersion != "risk-v1" {
		t.Errorf("requested event = (%q, %d, %q), want transfer.requested v1 risk-v1", eventType, aggregateVersion, policyVersion)
	}

	if _, err := fixture.pool.Exec(t.Context(), `UPDATE accounts SET status = 'frozen' WHERE id = $1`, sourceID); err != nil {
		t.Fatalf("freezing source account: %v", err)
	}
	replayed, err := repository.CreatePending(t.Context(), lifecycle)
	if err != nil {
		t.Fatalf("Repository.CreatePending(replay) error = %v", err)
	}
	if replayed.Status() != transferdomain.StatusPendingRisk || replayed.Version() != 1 {
		t.Errorf("CreatePending(replay) = (%s, %d), want pending risk v1", replayed.Status(), replayed.Version())
	}
	fixture.assertTransferCount(t, ownerID, pending.IdempotencyKey(), 1)
	fixture.assertOutboxEventCount(t, pending.ID(), 1)
}

func TestTransferRiskAssessmentSchema(t *testing.T) {
	fixture := newFixture(t)
	ownerID := fixture.newUUID(t)
	sourceID := fixture.createAccount(t, ownerID, "USD", "customer", "active")
	destinationID := fixture.createAccount(t, fixture.newUUID(t), "USD", "customer", "active")
	pending := fixture.transfer(t, ownerID, sourceID, destinationID, "risk-assessment", 40)
	if _, err := fixture.pool.Exec(
		t.Context(),
		`INSERT INTO transfers (
			id, requester_id, idempotency_key, source_account_id, destination_account_id,
			currency, amount_minor, requested_at, status, state_version, risk_policy_version
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending_risk', 1, 'risk-v1')`,
		pending.ID(),
		pending.RequesterID(),
		pending.IdempotencyKey(),
		pending.SourceAccountID(),
		pending.DestinationAccountID(),
		pending.Amount().Currency().String(),
		pending.Amount().MinorUnits(),
		pending.RequestedAt(),
	); err != nil {
		t.Fatalf("inserting pending transfer: %v", err)
	}
	if _, err := fixture.pool.Exec(
		t.Context(),
		`UPDATE transfers SET status = 'review_required', state_version = 2 WHERE id = $1`,
		pending.ID(),
	); err != nil {
		t.Fatalf("transitioning transfer for assessment: %v", err)
	}

	causationEventID := fixture.newUUID(t)
	if _, err := fixture.pool.Exec(
		t.Context(),
		`INSERT INTO transfer_risk_assessments (
			transfer_id, lifecycle_version, causation_event_id, policy_version,
			captured_input, score, decision, signals, assessed_at
		)
		VALUES ($1, 2, $2, 'risk-v1', $3, 600, 'review', $4, $5)`,
		pending.ID(),
		causationEventID,
		[]byte(`{"amount_minor":40,"currency":"USD"}`),
		[]byte(`[{"code":"amount_review_threshold","contribution":600}]`),
		pending.RequestedAt(),
	); err != nil {
		t.Fatalf("inserting risk assessment: %v", err)
	}

	if _, err := fixture.pool.Exec(
		t.Context(),
		`INSERT INTO transfer_risk_assessments (
			transfer_id, lifecycle_version, causation_event_id, policy_version,
			captured_input, score, decision, signals, assessed_at
		)
		VALUES ($1, 2, $2, 'risk-v1', $3, 600, 'review', $4, $5)`,
		pending.ID(),
		fixture.newUUID(t),
		[]byte(`{"amount_minor":40,"currency":"USD"}`),
		[]byte(`[{"code":"amount_review_threshold","contribution":600}]`),
		pending.RequestedAt(),
	); err == nil {
		t.Error("inserting duplicate transfer lifecycle assessment succeeded")
	}
}

func TestStoreRiskAssessmentTx(t *testing.T) {
	fixture := newFixture(t)
	ownerID := fixture.newUUID(t)
	sourceID := fixture.createAccount(t, ownerID, "USD", "customer", "active")
	destinationID := fixture.createAccount(t, fixture.newUUID(t), "USD", "customer", "active")
	pending := fixture.transfer(t, ownerID, sourceID, destinationID, "risk-transition", 60)
	if _, err := fixture.pool.Exec(
		t.Context(),
		`INSERT INTO transfers (
			id, requester_id, idempotency_key, source_account_id, destination_account_id,
			currency, amount_minor, requested_at, status, state_version, risk_policy_version
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending_risk', 1, 'risk-v1')`,
		pending.ID(),
		pending.RequesterID(),
		pending.IdempotencyKey(),
		pending.SourceAccountID(),
		pending.DestinationAccountID(),
		pending.Amount().Currency().String(),
		pending.Amount().MinorUnits(),
		pending.RequestedAt(),
	); err != nil {
		t.Fatalf("inserting pending transfer: %v", err)
	}

	policy, err := riskdomain.NewPolicy(riskdomain.PolicyParams{
		Version: "risk-v1",
		Thresholds: []riskdomain.Threshold{{
			Currency:           pending.Amount().Currency(),
			ReviewAmountMinor:  50,
			DeclineAmountMinor: 100,
		}},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	input := riskdomain.Input{Amount: pending.Amount(), VelocityTransferCount: 3}
	evaluation, err := policy.Evaluate(input)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}

	tx, err := fixture.pool.Begin(t.Context())
	if err != nil {
		t.Fatalf("beginning transaction: %v", err)
	}
	defer func() {
		if err := tx.Rollback(t.Context()); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			t.Errorf("rolling back transaction: %v", err)
		}
	}()
	current, err := transferpostgres.LoadLifecycleForUpdateTx(t.Context(), tx, pending.ID())
	if err != nil {
		t.Fatalf("LoadLifecycleForUpdateTx() error = %v", err)
	}
	next, err := current.RequireReview()
	if err != nil {
		t.Fatalf("RequireReview() error = %v", err)
	}
	causationID := fixture.newUUID(t)
	if err := transferpostgres.StoreRiskAssessmentTx(
		t.Context(), tx, current, next, input, evaluation, causationID, pending.RequestedAt(),
	); err != nil {
		t.Fatalf("StoreRiskAssessmentTx() error = %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("committing risk assessment: %v", err)
	}

	var status string
	var version, score int64
	var encodedInput, signals []byte
	if err := fixture.pool.QueryRow(
		t.Context(),
		`SELECT t.status, t.state_version, a.score, a.captured_input, a.signals
		 FROM transfers AS t
		 JOIN transfer_risk_assessments AS a ON a.transfer_id = t.id
		 WHERE t.id = $1`,
		pending.ID(),
	).Scan(&status, &version, &score, &encodedInput, &signals); err != nil {
		t.Fatalf("selecting persisted risk assessment: %v", err)
	}
	if status != "review_required" || version != 2 || score != 600 {
		t.Errorf("persisted risk assessment = (%q, %d, %d), want review_required version 2 score 600", status, version, score)
	}
	var caseVersion int64
	var caseStatus string
	var openedAt time.Time
	if err := fixture.pool.QueryRow(
		t.Context(),
		`SELECT assessment_lifecycle_version, status, opened_at
		 FROM transfer_review_cases
		 WHERE transfer_id = $1`,
		pending.ID(),
	).Scan(&caseVersion, &caseStatus, &openedAt); err != nil {
		t.Fatalf("selecting persisted review case: %v", err)
	}
	if caseVersion != 2 || caseStatus != "open" || !openedAt.Equal(pending.RequestedAt()) {
		t.Errorf("persisted review case = (%d, %q, %s), want (2, open, %s)", caseVersion, caseStatus, openedAt, pending.RequestedAt())
	}
	var storedInput struct {
		AmountMinor           int64  `json:"amount_minor"`
		Currency              string `json:"currency"`
		VelocityTransferCount int    `json:"velocity_transfer_count"`
		VelocityDegraded      bool   `json:"velocity_degraded"`
	}
	if err := json.Unmarshal(encodedInput, &storedInput); err != nil {
		t.Fatalf("decoding captured input: %v", err)
	}
	if storedInput.AmountMinor != 60 || storedInput.Currency != "USD" || storedInput.VelocityTransferCount != 3 || storedInput.VelocityDegraded {
		t.Errorf("captured input = %+v, want amount 60 USD and velocity count 3", storedInput)
	}
	var storedSignals []struct {
		Code         string `json:"code"`
		Contribution int    `json:"contribution"`
	}
	if err := json.Unmarshal(signals, &storedSignals); err != nil {
		t.Fatalf("decoding stored signals: %v", err)
	}
	if len(storedSignals) != 1 || storedSignals[0].Code != "amount_review_threshold" ||
		storedSignals[0].Contribution != 600 {
		t.Errorf("stored signals = %+v, want one amount review signal", storedSignals)
	}

	tx, err = fixture.pool.Begin(t.Context())
	if err != nil {
		t.Fatalf("beginning review decision transaction: %v", err)
	}
	current, err = transferpostgres.LoadLifecycleForUpdateTx(t.Context(), tx, pending.ID())
	if err != nil {
		t.Fatalf("loading review lifecycle: %v", err)
	}
	approved, err := current.Approve()
	if err != nil {
		t.Fatalf("approving review lifecycle: %v", err)
	}
	decidedAt := pending.RequestedAt().Add(time.Minute)
	if err := transferpostgres.StoreLifecycleTransitionTx(t.Context(), tx, current, approved); err != nil {
		t.Fatalf("storing review decision lifecycle: %v", err)
	}
	if err := transferpostgres.DecideReviewCaseTx(t.Context(), tx, pending.ID(), "approved", "analyst-1", decidedAt); err != nil {
		t.Fatalf("deciding review case: %v", err)
	}
	if err := auditpostgres.AppendReviewDecisionTx(t.Context(), tx, auditpostgres.ReviewDecision{
		TransferID:       pending.ID(),
		LifecycleVersion: approved.Version(),
		Decision:         "approved",
		ActorSubject:     "analyst-1",
		OccurredAt:       decidedAt,
	}); err != nil {
		t.Fatalf("appending review audit record: %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("committing review decision: %v", err)
	}
	var decision, decidedBy string
	if err := fixture.pool.QueryRow(
		t.Context(),
		`SELECT t.status, t.state_version, c.status, c.decision, c.decided_by, c.decided_at
		 FROM transfers AS t
		 JOIN transfer_review_cases AS c ON c.transfer_id = t.id
		 WHERE t.id = $1`,
		pending.ID(),
	).Scan(&status, &version, &caseStatus, &decision, &decidedBy, &openedAt); err != nil {
		t.Fatalf("selecting persisted review decision: %v", err)
	}
	if status != "approved" || version != 3 || caseStatus != "approved" || decision != "approved" || decidedBy != "analyst-1" || !openedAt.Equal(decidedAt) {
		t.Errorf("persisted review decision = (%q, %d, %q, %q, %q, %s), want approved lifecycle and case", status, version, caseStatus, decision, decidedBy, openedAt)
	}
	var auditVersion int64
	var auditDecision, auditSubject string
	var auditOccurredAt time.Time
	if err := fixture.pool.QueryRow(
		t.Context(),
		`SELECT lifecycle_version, decision, actor_subject, occurred_at
		 FROM transfer_review_audit_records
		 WHERE transfer_id = $1`,
		pending.ID(),
	).Scan(&auditVersion, &auditDecision, &auditSubject, &auditOccurredAt); err != nil {
		t.Fatalf("selecting transfer review audit record: %v", err)
	}
	if auditVersion != 3 || auditDecision != "approved" || auditSubject != "analyst-1" || !auditOccurredAt.Equal(decidedAt) {
		t.Errorf("audit record = (%d, %q, %q, %s), want approved analyst decision", auditVersion, auditDecision, auditSubject, auditOccurredAt)
	}
}

func TestRepositoryFailureDoesNotReserveKey(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	ownerID := fixture.newUUID(t)
	sourceID := fixture.createAccount(t, ownerID, "USD", "customer", "active")
	destinationID := fixture.createAccount(t, fixture.newUUID(t), "USD", "customer", "active")

	request := fixture.transfer(t, ownerID, sourceID, destinationID, "retryable-failure", 25)
	if _, err := repository.Create(t.Context(), request); !errors.Is(err, transferpostgres.ErrInsufficientFunds) {
		t.Fatalf("Repository.Create(unfunded) error = %v, want ErrInsufficientFunds", err)
	}
	fixture.assertTransferCount(t, ownerID, "retryable-failure", 0)
	fixture.assertOutboxEventCount(t, request.ID(), 0)

	fixture.fund(t, sourceID, 25)
	created, err := repository.Create(t.Context(), request)
	if err != nil {
		t.Fatalf("Repository.Create(retry) error = %v", err)
	}
	assertTransferEqual(t, created, request)
	fixture.assertBalance(t, sourceID, 0, 2)
	fixture.assertBalance(t, destinationID, 25, 1)
	fixture.assertOutboxEventCount(t, request.ID(), 1)
}

func TestRepositoryOutboxFailureRollsBackTransferAndLedger(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	ownerID := fixture.newUUID(t)
	sourceID := fixture.createAccount(t, ownerID, "USD", "customer", "active")
	destinationID := fixture.createAccount(t, fixture.newUUID(t), "USD", "customer", "active")
	fixture.fund(t, sourceID, 100)

	request := fixture.transfer(t, ownerID, sourceID, destinationID, "outbox-conflict", 30)
	draft, err := transferevents.Completed(request)
	if err != nil {
		t.Fatalf("events.Completed() error = %v", err)
	}
	tx, err := fixture.pool.Begin(t.Context())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	envelope, err := outboxpostgres.AddTx(t.Context(), tx, draft)
	if err != nil {
		if rollbackErr := tx.Rollback(t.Context()); rollbackErr != nil {
			t.Errorf("Rollback() after AddTx error = %v", rollbackErr)
		}
		t.Fatalf("AddTx() error = %v", err)
	}
	fixture.eventIDs = append(fixture.eventIDs, envelope.EventID())
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	if _, err := repository.Create(t.Context(), request); !errors.Is(err, outboxpostgres.ErrAlreadyExists) {
		t.Fatalf("Repository.Create() error = %v, want ErrAlreadyExists", err)
	}
	fixture.assertTransferCount(t, ownerID, "outbox-conflict", 0)
	fixture.assertBalance(t, sourceID, 100, 1)
	fixture.assertBalance(t, destinationID, 0, 0)
	fixture.assertJournalEntryCount(t, request.ID(), 0)
	fixture.assertOutboxEventCount(t, request.ID(), 1)
}

func TestRepositoryEnforcesAuthorizationAndAccountPolicy(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	ownerID := fixture.newUUID(t)
	otherOwnerID := fixture.newUUID(t)
	sourceID := fixture.createAccount(t, ownerID, "USD", "customer", "active")
	destinationID := fixture.createAccount(t, otherOwnerID, "USD", "customer", "active")
	fixture.fund(t, sourceID, 100)

	unauthorized := fixture.transfer(t, otherOwnerID, sourceID, destinationID, "unauthorized", 10)
	if _, err := repository.Create(t.Context(), unauthorized); !errors.Is(err, transferpostgres.ErrUnauthorized) {
		t.Fatalf("Repository.Create(unauthorized) error = %v, want ErrUnauthorized", err)
	}

	frozenID := fixture.createAccount(t, fixture.newUUID(t), "USD", "customer", "frozen")
	frozen := fixture.transfer(t, ownerID, sourceID, frozenID, "frozen", 10)
	if _, err := repository.Create(t.Context(), frozen); !errors.Is(err, transferpostgres.ErrInactiveAccount) {
		t.Fatalf("Repository.Create(frozen) error = %v, want ErrInactiveAccount", err)
	}

	systemID := fixture.createAccount(t, fixture.newUUID(t), "USD", "system", "active")
	system := fixture.transfer(t, ownerID, sourceID, systemID, "system", 10)
	if _, err := repository.Create(t.Context(), system); !errors.Is(err, transferpostgres.ErrNonCustomerAccount) {
		t.Fatalf("Repository.Create(system) error = %v, want ErrNonCustomerAccount", err)
	}

	eurDestinationID := fixture.createAccount(t, fixture.newUUID(t), "EUR", "customer", "active")
	currencyMismatch := fixture.transfer(t, ownerID, sourceID, eurDestinationID, "currency", 10)
	if _, err := repository.Create(t.Context(), currencyMismatch); !errors.Is(err, transferpostgres.ErrCurrencyMismatch) {
		t.Fatalf("Repository.Create(currency mismatch) error = %v, want ErrCurrencyMismatch", err)
	}

	missing := fixture.transfer(t, ownerID, sourceID, fixture.newUUID(t), "missing", 10)
	if _, err := repository.Create(t.Context(), missing); !errors.Is(err, transferpostgres.ErrNotFound) {
		t.Fatalf("Repository.Create(missing) error = %v, want ErrNotFound", err)
	}
	fixture.assertBalance(t, sourceID, 100, 1)
}

func TestRepositoryConcurrentReplayHasOneEffect(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	ownerID := fixture.newUUID(t)
	sourceID := fixture.createAccount(t, ownerID, "USD", "customer", "active")
	destinationID := fixture.createAccount(t, fixture.newUUID(t), "USD", "customer", "active")
	fixture.fund(t, sourceID, 100)

	const workers = 8
	results := make(chan transferdomain.Transfer, workers)
	errorsCh := make(chan error, workers)
	requests := make([]transferdomain.Transfer, workers)
	for index := range requests {
		requests[index] = fixture.transfer(t, ownerID, sourceID, destinationID, "concurrent", 25)
	}
	var group sync.WaitGroup
	for _, request := range requests {
		group.Add(1)
		go func(request transferdomain.Transfer) {
			defer group.Done()
			created, err := repository.Create(context.Background(), request)
			if err != nil {
				errorsCh <- err
				return
			}
			results <- created
		}(request)
	}
	group.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		t.Errorf("Repository.Create(concurrent) error = %v", err)
	}
	var winner transferdomain.Transfer
	for result := range results {
		if winner.ID() == "" {
			winner = result
			continue
		}
		assertTransferEqual(t, result, winner)
	}
	fixture.assertTransferCount(t, ownerID, "concurrent", 1)
	fixture.assertBalance(t, sourceID, 75, 2)
	fixture.assertBalance(t, destinationID, 25, 1)
	fixture.assertOutboxEventCount(t, winner.ID(), 1)
}

func TestRepositoryConcurrentDifferentKeysCannotOverdraw(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	ownerID := fixture.newUUID(t)
	sourceID := fixture.createAccount(t, ownerID, "USD", "customer", "active")
	destinationA := fixture.createAccount(t, fixture.newUUID(t), "USD", "customer", "active")
	destinationB := fixture.createAccount(t, fixture.newUUID(t), "USD", "customer", "active")
	fixture.fund(t, sourceID, 100)
	requests := []transferdomain.Transfer{
		fixture.transfer(t, ownerID, sourceID, destinationA, "withdrawal-a", 80),
		fixture.transfer(t, ownerID, sourceID, destinationB, "withdrawal-b", 80),
	}

	errorsCh := make(chan error, len(requests))
	var group sync.WaitGroup
	for _, request := range requests {
		group.Add(1)
		go func(request transferdomain.Transfer) {
			defer group.Done()
			_, err := repository.Create(context.Background(), request)
			errorsCh <- err
		}(request)
	}
	group.Wait()
	close(errorsCh)

	var succeeded, insufficient int
	for err := range errorsCh {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, transferpostgres.ErrInsufficientFunds):
			insufficient++
		default:
			t.Errorf("Repository.Create(concurrent withdrawal) error = %v", err)
		}
	}
	if succeeded != 1 || insufficient != 1 {
		t.Errorf("outcomes = (%d success, %d insufficient), want (1, 1)", succeeded, insufficient)
	}
	fixture.assertBalance(t, sourceID, 20, 2)
	if got := fixture.balance(t, destinationA) + fixture.balance(t, destinationB); got != 80 {
		t.Errorf("destination balance total = %d, want 80", got)
	}
}

type fixture struct {
	pool       *pgxpool.Pool
	walletIDs  []string
	accountIDs []string
	eventIDs   []string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for integration tests")
	}
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	fixture := &fixture{pool: pool}
	t.Cleanup(func() {
		fixture.cleanup(t)
		pool.Close()
	})
	return fixture
}

func mustRepository(t testing.TB, pool *pgxpool.Pool) *transferpostgres.Repository {
	t.Helper()
	repository, err := transferpostgres.NewRepository(pool)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	return repository
}

func (f *fixture) newUUID(t testing.TB) string {
	t.Helper()
	var id string
	if err := f.pool.QueryRow(context.Background(), `SELECT gen_random_uuid()::text`).Scan(&id); err != nil {
		t.Fatalf("generating UUID: %v", err)
	}
	return id
}

func (f *fixture) createAccount(
	t testing.TB,
	ownerID, currency, accountType, status string,
) string {
	t.Helper()
	var walletID string
	if err := f.pool.QueryRow(
		context.Background(),
		`INSERT INTO wallets (owner_id) VALUES ($1) RETURNING id::text`,
		ownerID,
	).Scan(&walletID); err != nil {
		t.Fatalf("creating wallet: %v", err)
	}
	var accountID string
	if err := f.pool.QueryRow(
		context.Background(),
		`INSERT INTO accounts (wallet_id, currency, account_type, status)
         VALUES ($1, $2, $3, $4) RETURNING id::text`,
		walletID,
		currency,
		accountType,
		status,
	).Scan(&accountID); err != nil {
		t.Fatalf("creating account: %v", err)
	}
	if _, err := f.pool.Exec(
		context.Background(),
		`INSERT INTO account_balances (account_id, currency, account_type) VALUES ($1, $2, $3)`,
		accountID,
		currency,
		accountType,
	); err != nil {
		t.Fatalf("creating balance: %v", err)
	}
	f.walletIDs = append(f.walletIDs, walletID)
	f.accountIDs = append(f.accountIDs, accountID)
	return accountID
}

func (f *fixture) fund(t testing.TB, accountID string, amount int64) {
	t.Helper()
	systemID := f.createAccount(t, f.newUUID(t), "USD", "system", "active")
	entry := mustEntry(t, f.newUUID(t), systemID, accountID, amount)
	repository, err := ledgerpostgres.NewRepository(f.pool)
	if err != nil {
		t.Fatalf("ledger NewRepository() error = %v", err)
	}
	if err := repository.Post(context.Background(), entry); err != nil {
		t.Fatalf("funding account: %v", err)
	}
}

func (f *fixture) transfer(
	t testing.TB,
	requesterID, sourceID, destinationID, key string,
	amount int64,
) transferdomain.Transfer {
	t.Helper()
	return mustTransfer(t, transferdomain.NewTransferParams{
		ID:                   f.newUUID(t),
		IdempotencyKey:       key,
		RequesterID:          requesterID,
		SourceAccountID:      sourceID,
		DestinationAccountID: destinationID,
		Amount:               mustMoney(t, amount, "USD"),
		RequestedAt:          time.Now().UTC().Truncate(time.Microsecond),
	})
}

func (f *fixture) assertBalance(t testing.TB, accountID string, balance, version int64) {
	t.Helper()
	var gotBalance, gotVersion int64
	if err := f.pool.QueryRow(
		context.Background(),
		`SELECT balance_minor, version FROM account_balances WHERE account_id = $1`,
		accountID,
	).Scan(&gotBalance, &gotVersion); err != nil {
		t.Fatalf("selecting balance: %v", err)
	}
	if gotBalance != balance || gotVersion != version {
		t.Errorf("balance = (%d, %d), want (%d, %d)", gotBalance, gotVersion, balance, version)
	}
}

func (f *fixture) balance(t testing.TB, accountID string) int64 {
	t.Helper()
	var balance int64
	if err := f.pool.QueryRow(
		context.Background(),
		`SELECT balance_minor FROM account_balances WHERE account_id = $1`,
		accountID,
	).Scan(&balance); err != nil {
		t.Fatalf("selecting balance: %v", err)
	}
	return balance
}

func (f *fixture) assertTransferCount(t testing.TB, requesterID, key string, want int) {
	t.Helper()
	var got int
	if err := f.pool.QueryRow(
		context.Background(),
		`SELECT count(*) FROM transfers WHERE requester_id = $1 AND idempotency_key = $2`,
		requesterID,
		key,
	).Scan(&got); err != nil {
		t.Fatalf("counting transfers: %v", err)
	}
	if got != want {
		t.Errorf("transfer count = %d, want %d", got, want)
	}
}

func (f *fixture) assertOutboxEventCount(t testing.TB, transferID string, want int) {
	t.Helper()
	var got int
	if err := f.pool.QueryRow(
		context.Background(),
		`SELECT count(*) FROM outbox_events WHERE aggregate_type = 'transfer' AND aggregate_id = $1`,
		transferID,
	).Scan(&got); err != nil {
		t.Fatalf("counting transfer outbox events: %v", err)
	}
	if got != want {
		t.Errorf("transfer outbox event count = %d, want %d", got, want)
	}
}

func (f *fixture) assertCompletedEvent(t testing.TB, transfer transferdomain.Transfer) {
	t.Helper()
	var eventType, aggregateType, aggregateID, correlationID string
	var eventVersion int16
	var aggregateVersion int64
	var occurredAt time.Time
	var payload []byte
	if err := f.pool.QueryRow(
		context.Background(),
		`SELECT event_type, event_version, aggregate_type, aggregate_id::text,
                aggregate_version, correlation_id::text, occurred_at, payload
         FROM outbox_events
         WHERE aggregate_type = 'transfer' AND aggregate_id = $1`,
		transfer.ID(),
	).Scan(
		&eventType,
		&eventVersion,
		&aggregateType,
		&aggregateID,
		&aggregateVersion,
		&correlationID,
		&occurredAt,
		&payload,
	); err != nil {
		t.Fatalf("selecting completed transfer event: %v", err)
	}
	if eventType != transferevents.CompletedType || eventVersion != transferevents.CompletedVersion ||
		aggregateType != "transfer" || aggregateID != transfer.ID() || aggregateVersion != 1 ||
		correlationID != transfer.ID() || !occurredAt.Equal(transfer.RequestedAt()) {
		t.Errorf("completed event metadata does not match transfer")
	}
	var got struct {
		TransferID           string `json:"transfer_id"`
		RequesterID          string `json:"requester_id"`
		SourceAccountID      string `json:"source_account_id"`
		DestinationAccountID string `json:"destination_account_id"`
		Currency             string `json:"currency"`
		AmountMinor          int64  `json:"amount_minor"`
		RequestedAt          string `json:"requested_at"`
	}
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("decoding completed transfer payload: %v", err)
	}
	if got.TransferID != transfer.ID() || got.RequesterID != transfer.RequesterID() ||
		got.SourceAccountID != transfer.SourceAccountID() ||
		got.DestinationAccountID != transfer.DestinationAccountID() ||
		got.Currency != transfer.Amount().Currency().String() ||
		got.AmountMinor != transfer.Amount().MinorUnits() ||
		got.RequestedAt != transfer.RequestedAt().Format(time.RFC3339Nano) {
		t.Errorf("completed event payload = %+v, want transfer data", got)
	}
}

func (f *fixture) assertJournalEntryCount(t testing.TB, entryID string, want int) {
	t.Helper()
	var got int
	if err := f.pool.QueryRow(
		context.Background(),
		`SELECT count(*) FROM journal_entries WHERE id = $1`,
		entryID,
	).Scan(&got); err != nil {
		t.Fatalf("counting journal entries: %v", err)
	}
	if got != want {
		t.Errorf("journal entry count = %d, want %d", got, want)
	}
}

func (f *fixture) cleanup(t testing.TB) {
	t.Helper()
	ctx := context.Background()
	if _, err := f.pool.Exec(
		ctx,
		`DELETE FROM transfer_review_audit_records
         WHERE transfer_id IN (
               SELECT id FROM transfers WHERE source_account_id = ANY($1::uuid[])
         )`,
		f.accountIDs,
	); err != nil {
		t.Errorf("cleaning transfer review audit records: %v", err)
	}
	if _, err := f.pool.Exec(
		ctx,
		`DELETE FROM outbox_events
         WHERE aggregate_type = 'transfer'
           AND aggregate_id IN (
               SELECT id FROM transfers WHERE source_account_id = ANY($1::uuid[])
           )`,
		f.accountIDs,
	); err != nil {
		t.Errorf("cleaning transfer outbox events: %v", err)
	}
	if _, err := f.pool.Exec(
		ctx,
		`DELETE FROM transfer_review_cases
         WHERE transfer_id IN (
               SELECT id FROM transfers WHERE source_account_id = ANY($1::uuid[])
         )`,
		f.accountIDs,
	); err != nil {
		t.Errorf("cleaning transfer review cases: %v", err)
	}
	if _, err := f.pool.Exec(
		ctx,
		`DELETE FROM transfer_risk_assessments
         WHERE transfer_id IN (
               SELECT id FROM transfers WHERE source_account_id = ANY($1::uuid[])
         )`,
		f.accountIDs,
	); err != nil {
		t.Errorf("cleaning transfer risk assessments: %v", err)
	}
	if len(f.eventIDs) > 0 {
		if _, err := f.pool.Exec(
			ctx,
			`DELETE FROM outbox_events WHERE event_id = ANY($1::uuid[])`,
			f.eventIDs,
		); err != nil {
			t.Errorf("cleaning tracked outbox events: %v", err)
		}
	}
	if _, err := f.pool.Exec(ctx, `DELETE FROM transfers WHERE source_account_id = ANY($1::uuid[])`, f.accountIDs); err != nil {
		t.Errorf("cleaning transfers: %v", err)
	}
	rows, err := f.pool.Query(
		ctx,
		`SELECT DISTINCT journal_entry_id::text FROM postings WHERE account_id = ANY($1::uuid[])`,
		f.accountIDs,
	)
	if err != nil {
		t.Errorf("finding ledger entries: %v", err)
	} else {
		entryIDs := []string{}
		for rows.Next() {
			var entryID string
			if scanErr := rows.Scan(&entryID); scanErr != nil {
				t.Errorf("scanning ledger entry: %v", scanErr)
				break
			}
			entryIDs = append(entryIDs, entryID)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Errorf("iterating ledger entries: %v", rowsErr)
		}
		rows.Close()
		if len(entryIDs) > 0 {
			if _, deleteErr := f.pool.Exec(
				ctx,
				`DELETE FROM postings WHERE journal_entry_id = ANY($1::uuid[])`,
				entryIDs,
			); deleteErr != nil {
				t.Errorf("cleaning postings: %v", deleteErr)
			}
			if _, deleteErr := f.pool.Exec(
				ctx,
				`DELETE FROM journal_entries WHERE id = ANY($1::uuid[])`,
				entryIDs,
			); deleteErr != nil {
				t.Errorf("cleaning journal entries: %v", deleteErr)
			}
		}
	}
	if _, err := f.pool.Exec(ctx, `DELETE FROM wallets WHERE id = ANY($1::uuid[])`, f.walletIDs); err != nil {
		t.Errorf("cleaning wallets: %v", err)
	}
}

func mustEntry(
	t testing.TB,
	id, sourceID, destinationID string,
	amount int64,
) ledgerdomain.JournalEntry {
	t.Helper()
	entry, err := ledgerdomain.NewJournalEntry(ledgerdomain.NewJournalEntryParams{
		ID: id,
		Postings: []ledgerdomain.Posting{
			mustPosting(t, sourceID, -amount),
			mustPosting(t, destinationID, amount),
		},
		RecordedAt: time.Now().UTC().Truncate(time.Microsecond),
	})
	if err != nil {
		t.Fatalf("NewJournalEntry() error = %v", err)
	}
	return entry
}

func mustPosting(t testing.TB, accountID string, amount int64) ledgerdomain.Posting {
	t.Helper()
	posting, err := ledgerdomain.NewPosting(accountID, mustMoney(t, amount, "USD"))
	if err != nil {
		t.Fatalf("NewPosting() error = %v", err)
	}
	return posting
}

func assertTransferEqual(t testing.TB, got, want transferdomain.Transfer) {
	t.Helper()
	if got.ID() != want.ID() ||
		got.IdempotencyKey() != want.IdempotencyKey() ||
		got.RequesterID() != want.RequesterID() ||
		got.SourceAccountID() != want.SourceAccountID() ||
		got.DestinationAccountID() != want.DestinationAccountID() ||
		got.Amount() != want.Amount() ||
		!got.RequestedAt().Equal(want.RequestedAt()) {
		t.Errorf("transfer = %+v, want %+v", got, want)
	}
}

func mustTransfer(t testing.TB, params transferdomain.NewTransferParams) transferdomain.Transfer {
	t.Helper()
	transfer, err := transferdomain.NewTransfer(params)
	if err != nil {
		t.Fatalf("NewTransfer() error = %v", err)
	}
	return transfer
}

func mustMoney(t testing.TB, minorUnits int64, currencyCode string) ledgerdomain.Money {
	t.Helper()
	currency, err := ledgerdomain.ParseCurrency(currencyCode)
	if err != nil {
		t.Fatalf("ParseCurrency() error = %v", err)
	}
	money, err := ledgerdomain.NewMoney(minorUnits, currency)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	return money
}
