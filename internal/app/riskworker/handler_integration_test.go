//go:build integration

package riskworker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	riskdomain "github.com/krav01/intelligent-wallet-ledger/internal/risk/domain"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
	transferevents "github.com/krav01/intelligent-wallet-ledger/internal/transfer/events"
	transferpostgres "github.com/krav01/intelligent-wallet-ledger/internal/transfer/postgres"
)

func TestHandlerHandleDeduplicatesRedelivery(t *testing.T) {
	fixture := newIntegrationFixture(t)
	lifecycle, envelope := fixture.createPendingRequested(t, 60)
	handler := fixture.handler(t)

	if err := handler.Handle(t.Context(), envelope); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if err := handler.Handle(t.Context(), envelope); err != nil {
		t.Fatalf("Handle(redelivery) error = %v", err)
	}

	fixture.assertLifecycle(t, lifecycle.Transfer().ID(), "review_required", 2)
	fixture.assertCount(t, `SELECT count(*) FROM transfer_risk_assessments WHERE transfer_id = $1`, lifecycle.Transfer().ID(), 1)
	fixture.assertCount(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'transfer.risk_assessed'`, lifecycle.Transfer().ID(), 1)
	fixture.assertCount(t, `SELECT count(*) FROM consumer_inbox WHERE consumer_name = 'risk-worker.v1' AND event_id = $1`, envelope.EventID(), 1)
	fixture.assertBalance(t, lifecycle.Transfer().SourceAccountID(), 0, 0)
	fixture.assertBalance(t, lifecycle.Transfer().DestinationAccountID(), 0, 0)
}

func TestHandlerHandleCapturesVelocityOnceAcrossRedelivery(t *testing.T) {
	fixture := newIntegrationFixture(t)
	lifecycle, envelope := fixture.createPendingRequested(t, 40)
	observer := &fakeVelocityObserver{count: 3}
	handler := fixture.handlerWithVelocity(t, observer)

	if err := handler.Handle(t.Context(), envelope); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if err := handler.Handle(t.Context(), envelope); err != nil {
		t.Fatalf("Handle(redelivery) error = %v", err)
	}
	if observer.calls != 1 {
		t.Errorf("velocity observer calls = %d, want 1", observer.calls)
	}
	var input struct {
		VelocityTransferCount int  `json:"velocity_transfer_count"`
		VelocityDegraded      bool `json:"velocity_degraded"`
	}
	var encodedInput []byte
	if err := fixture.pool.QueryRow(t.Context(), `SELECT captured_input FROM transfer_risk_assessments WHERE transfer_id = $1`, lifecycle.Transfer().ID()).Scan(&encodedInput); err != nil {
		t.Fatalf("selecting captured input: %v", err)
	}
	if err := json.Unmarshal(encodedInput, &input); err != nil {
		t.Fatalf("decoding captured input: %v", err)
	}
	if input.VelocityTransferCount != 3 || input.VelocityDegraded {
		t.Errorf("captured input = %+v, want count 3 and non-degraded observation", input)
	}
	fixture.assertLifecycle(t, lifecycle.Transfer().ID(), "review_required", 2)
}

func TestHandlerHandleRollsBackMismatchedRequestedTransfer(t *testing.T) {
	fixture := newIntegrationFixture(t)
	lifecycle, _ := fixture.createPendingRequested(t, 60)
	mismatched := fixture.pendingLifecycle(t, lifecycle.Transfer().ID(), lifecycle.Transfer().RequesterID(), lifecycle.Transfer().SourceAccountID(), lifecycle.Transfer().DestinationAccountID(), 61, lifecycle.Transfer().RequestedAt())
	draft, err := transferevents.Requested(mismatched)
	if err != nil {
		t.Fatalf("Requested() error = %v", err)
	}
	envelope, err := event.NewEnvelope(fixture.newUUID(t), draft)
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}
	fixture.eventIDs = append(fixture.eventIDs, envelope.EventID())

	err = fixture.handler(t).Handle(t.Context(), envelope)
	if !errors.Is(err, transferpostgres.ErrStateConflict) {
		t.Fatalf("Handle() error = %v, want ErrStateConflict", err)
	}

	fixture.assertLifecycle(t, lifecycle.Transfer().ID(), "pending_risk", 1)
	fixture.assertCount(t, `SELECT count(*) FROM transfer_risk_assessments WHERE transfer_id = $1`, lifecycle.Transfer().ID(), 0)
	fixture.assertCount(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'transfer.risk_assessed'`, lifecycle.Transfer().ID(), 0)
	fixture.assertCount(t, `SELECT count(*) FROM consumer_inbox WHERE consumer_name = 'risk-worker.v1' AND event_id = $1`, envelope.EventID(), 0)
}

type integrationFixture struct {
	pool        *pgxpool.Pool
	transferIDs []string
	accountIDs  []string
	walletIDs   []string
	eventIDs    []string
}

func newIntegrationFixture(t *testing.T) *integrationFixture {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for integration tests")
	}
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	fixture := &integrationFixture{pool: pool}
	t.Cleanup(func() {
		fixture.cleanup(t)
		pool.Close()
	})
	return fixture
}

func (f *integrationFixture) handler(t testing.TB) *Handler {
	t.Helper()
	currency, err := ledgerdomain.ParseCurrency("USD")
	if err != nil {
		t.Fatalf("ParseCurrency() error = %v", err)
	}
	policy, err := riskdomain.NewPolicy(riskdomain.PolicyParams{
		Version: "risk-v1",
		Thresholds: []riskdomain.Threshold{{
			Currency:           currency,
			ReviewAmountMinor:  50,
			DeclineAmountMinor: 100,
		}},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	handler, err := NewHandler(f.pool, policy, time.Now)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	return handler
}

func (f *integrationFixture) handlerWithVelocity(t testing.TB, velocity VelocityObserver) *Handler {
	t.Helper()
	currency, err := ledgerdomain.ParseCurrency("USD")
	if err != nil {
		t.Fatalf("ParseCurrency() error = %v", err)
	}
	policy, err := riskdomain.NewPolicy(riskdomain.PolicyParams{
		Version: "risk-v1",
		Thresholds: []riskdomain.Threshold{{
			Currency:                    currency,
			ReviewAmountMinor:           50,
			DeclineAmountMinor:          100,
			VelocityReviewTransferCount: 3,
		}},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	handler, err := NewHandlerWithPoliciesAndVelocity(f.pool, []riskdomain.Policy{policy}, velocity, slog.New(slog.DiscardHandler), time.Now)
	if err != nil {
		t.Fatalf("NewHandlerWithPoliciesAndVelocity() error = %v", err)
	}
	return handler
}

func (f *integrationFixture) createPendingRequested(t *testing.T, amount int64) (transferdomain.Lifecycle, event.Envelope) {
	t.Helper()
	requesterID := f.newUUID(t)
	sourceID := f.createAccount(t, requesterID)
	destinationID := f.createAccount(t, f.newUUID(t))
	lifecycle := f.pendingLifecycle(t, f.newUUID(t), requesterID, sourceID, destinationID, amount, time.Now().UTC().Truncate(time.Microsecond))
	repository, err := transferpostgres.NewRepository(f.pool)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	if _, err := repository.CreatePending(t.Context(), lifecycle); err != nil {
		t.Fatalf("CreatePending() error = %v", err)
	}
	f.transferIDs = append(f.transferIDs, lifecycle.Transfer().ID())

	envelope := f.requestedEnvelope(t, lifecycle.Transfer().ID())
	f.eventIDs = append(f.eventIDs, envelope.EventID())
	return lifecycle, envelope
}

func (f *integrationFixture) pendingLifecycle(
	t testing.TB,
	transferID, requesterID, sourceID, destinationID string,
	amount int64,
	requestedAt time.Time,
) transferdomain.Lifecycle {
	t.Helper()
	currency, err := ledgerdomain.ParseCurrency("USD")
	if err != nil {
		t.Fatalf("ParseCurrency() error = %v", err)
	}
	money, err := ledgerdomain.NewMoney(amount, currency)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	transfer, err := transferdomain.NewTransfer(transferdomain.NewTransferParams{
		ID:                   transferID,
		IdempotencyKey:       "risk-worker-integration",
		RequesterID:          requesterID,
		SourceAccountID:      sourceID,
		DestinationAccountID: destinationID,
		Amount:               money,
		RequestedAt:          requestedAt,
	})
	if err != nil {
		t.Fatalf("NewTransfer() error = %v", err)
	}
	lifecycle, err := transferdomain.NewPendingLifecycle(transfer, "risk-v1")
	if err != nil {
		t.Fatalf("NewPendingLifecycle() error = %v", err)
	}
	return lifecycle
}

func (f *integrationFixture) requestedEnvelope(t testing.TB, transferID string) event.Envelope {
	t.Helper()
	var (
		eventID, eventType, aggregateType, aggregateID, correlationID string
		eventVersion                                                  int16
		aggregateVersion                                              int64
		causationID                                                   *string
		occurredAt                                                    time.Time
		payload                                                       []byte
	)
	err := f.pool.QueryRow(
		context.Background(),
		`SELECT event_id::text, event_type, event_version, aggregate_type, aggregate_id::text,
		        aggregate_version, correlation_id::text, causation_id::text, occurred_at, payload
		 FROM outbox_events
		 WHERE aggregate_id = $1 AND event_type = 'transfer.requested'`,
		transferID,
	).Scan(&eventID, &eventType, &eventVersion, &aggregateType, &aggregateID, &aggregateVersion, &correlationID, &causationID, &occurredAt, &payload)
	if err != nil {
		t.Fatalf("selecting requested outbox event: %v", err)
	}
	draft, err := event.NewDraft(event.DraftParams{
		EventType:        eventType,
		EventVersion:     eventVersion,
		AggregateType:    aggregateType,
		AggregateID:      aggregateID,
		AggregateVersion: aggregateVersion,
		CorrelationID:    correlationID,
		OccurredAt:       occurredAt,
		Payload:          payload,
	})
	if err != nil {
		t.Fatalf("NewDraft() error = %v", err)
	}
	if causationID != nil {
		t.Fatalf("requested event causation ID = %q, want empty", *causationID)
	}
	envelope, err := event.NewEnvelope(eventID, draft)
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}
	return envelope
}

func (f *integrationFixture) createAccount(t testing.TB, ownerID string) string {
	t.Helper()
	var walletID string
	if err := f.pool.QueryRow(context.Background(), `INSERT INTO wallets (owner_id) VALUES ($1) RETURNING id::text`, ownerID).Scan(&walletID); err != nil {
		t.Fatalf("creating wallet: %v", err)
	}
	var accountID string
	if err := f.pool.QueryRow(context.Background(), `INSERT INTO accounts (wallet_id, currency, account_type, status) VALUES ($1, 'USD', 'customer', 'active') RETURNING id::text`, walletID).Scan(&accountID); err != nil {
		t.Fatalf("creating account: %v", err)
	}
	if _, err := f.pool.Exec(context.Background(), `INSERT INTO account_balances (account_id, currency, account_type) VALUES ($1, 'USD', 'customer')`, accountID); err != nil {
		t.Fatalf("creating balance: %v", err)
	}
	f.walletIDs = append(f.walletIDs, walletID)
	f.accountIDs = append(f.accountIDs, accountID)
	return accountID
}

func (f *integrationFixture) newUUID(t testing.TB) string {
	t.Helper()
	var id string
	if err := f.pool.QueryRow(context.Background(), `SELECT gen_random_uuid()::text`).Scan(&id); err != nil {
		t.Fatalf("generating UUID: %v", err)
	}
	return id
}

func (f *integrationFixture) assertLifecycle(t testing.TB, transferID, wantStatus string, wantVersion int64) {
	t.Helper()
	var status string
	var version int64
	if err := f.pool.QueryRow(context.Background(), `SELECT status, state_version FROM transfers WHERE id = $1`, transferID).Scan(&status, &version); err != nil {
		t.Fatalf("selecting transfer lifecycle: %v", err)
	}
	if status != wantStatus || version != wantVersion {
		t.Errorf("transfer lifecycle = (%q, %d), want (%q, %d)", status, version, wantStatus, wantVersion)
	}
}

func (f *integrationFixture) assertCount(t testing.TB, query, argument string, want int) {
	t.Helper()
	var got int
	if err := f.pool.QueryRow(context.Background(), query, argument).Scan(&got); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if got != want {
		t.Errorf("row count = %d, want %d", got, want)
	}
}

func (f *integrationFixture) assertBalance(t testing.TB, accountID string, wantBalance, wantVersion int64) {
	t.Helper()
	var balance, version int64
	if err := f.pool.QueryRow(context.Background(), `SELECT balance_minor, version FROM account_balances WHERE account_id = $1`, accountID).Scan(&balance, &version); err != nil {
		t.Fatalf("selecting balance: %v", err)
	}
	if balance != wantBalance || version != wantVersion {
		t.Errorf("balance = (%d, %d), want (%d, %d)", balance, version, wantBalance, wantVersion)
	}
}

func (f *integrationFixture) cleanup(t testing.TB) {
	t.Helper()
	ctx := context.Background()
	if len(f.eventIDs) > 0 {
		if _, err := f.pool.Exec(ctx, `DELETE FROM consumer_inbox WHERE event_id = ANY($1::uuid[])`, f.eventIDs); err != nil {
			t.Errorf("cleaning consumer inbox: %v", err)
		}
	}
	if len(f.transferIDs) > 0 {
		if _, err := f.pool.Exec(ctx, `DELETE FROM outbox_events WHERE aggregate_id = ANY($1::uuid[])`, f.transferIDs); err != nil {
			t.Errorf("cleaning transfer outbox events: %v", err)
		}
		if _, err := f.pool.Exec(ctx, `DELETE FROM transfer_risk_assessments WHERE transfer_id = ANY($1::uuid[])`, f.transferIDs); err != nil {
			t.Errorf("cleaning risk assessments: %v", err)
		}
		if _, err := f.pool.Exec(ctx, `DELETE FROM transfers WHERE id = ANY($1::uuid[])`, f.transferIDs); err != nil {
			t.Errorf("cleaning transfers: %v", err)
		}
	}
	if len(f.accountIDs) > 0 {
		if _, err := f.pool.Exec(ctx, `DELETE FROM account_balances WHERE account_id = ANY($1::uuid[])`, f.accountIDs); err != nil {
			t.Errorf("cleaning account balances: %v", err)
		}
		if _, err := f.pool.Exec(ctx, `DELETE FROM accounts WHERE id = ANY($1::uuid[])`, f.accountIDs); err != nil {
			t.Errorf("cleaning accounts: %v", err)
		}
	}
	if len(f.walletIDs) > 0 {
		if _, err := f.pool.Exec(ctx, `DELETE FROM wallets WHERE id = ANY($1::uuid[])`, f.walletIDs); err != nil {
			t.Errorf("cleaning wallets: %v", err)
		}
	}
}
