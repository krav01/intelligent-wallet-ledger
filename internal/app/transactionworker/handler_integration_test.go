//go:build integration

package transactionworker

import (
	"context"
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

func TestHandlerHandlePostsApprovedTransferOnce(t *testing.T) {
	fixture := newIntegrationFixture(t)
	lifecycle, envelope := fixture.approvedAssessment(t, 60, 100)
	handler := fixture.handler(t)

	if err := handler.Handle(t.Context(), envelope); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if err := handler.Handle(t.Context(), envelope); err != nil {
		t.Fatalf("Handle(redelivery) error = %v", err)
	}

	fixture.assertLifecycle(t, lifecycle.Transfer().ID(), "completed", 3, "")
	fixture.assertCount(t, `SELECT count(*) FROM journal_entries WHERE id = $1`, lifecycle.Transfer().ID(), 1)
	fixture.assertCount(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'transfer.completed'`, lifecycle.Transfer().ID(), 1)
	fixture.assertCount(t, `SELECT count(*) FROM consumer_inbox WHERE consumer_name = 'transaction-worker.v1' AND event_id = $1`, envelope.EventID(), 1)
	fixture.assertBalance(t, lifecycle.Transfer().SourceAccountID(), 40, 1)
	fixture.assertBalance(t, lifecycle.Transfer().DestinationAccountID(), 60, 1)
}

func TestHandlerHandleFailsInsufficientFunds(t *testing.T) {
	fixture := newIntegrationFixture(t)
	lifecycle, envelope := fixture.approvedAssessment(t, 60, 0)

	if err := fixture.handler(t).Handle(t.Context(), envelope); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	fixture.assertLifecycle(t, lifecycle.Transfer().ID(), "failed", 3, insufficientFundsReason)
	fixture.assertCount(t, `SELECT count(*) FROM journal_entries WHERE id = $1`, lifecycle.Transfer().ID(), 0)
	fixture.assertCount(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'transfer.failed'`, lifecycle.Transfer().ID(), 1)
	fixture.assertBalance(t, lifecycle.Transfer().SourceAccountID(), 0, 0)
	fixture.assertBalance(t, lifecycle.Transfer().DestinationAccountID(), 0, 0)
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
	handler, err := NewHandler(f.pool, func() time.Time {
		return time.Date(2026, time.September, 12, 12, 0, 0, 0, time.UTC)
	})
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}
	return handler
}

func (f *integrationFixture) approvedAssessment(t testing.TB, amount, sourceBalance int64) (transferdomain.Lifecycle, event.Envelope) {
	t.Helper()
	requesterID := f.newUUID(t)
	sourceID := f.createAccount(t, requesterID)
	destinationID := f.createAccount(t, f.newUUID(t))
	if _, err := f.pool.Exec(t.Context(), `UPDATE account_balances SET balance_minor = $2 WHERE account_id = $1`, sourceID, sourceBalance); err != nil {
		t.Fatalf("funding source balance: %v", err)
	}

	currency, err := ledgerdomain.ParseCurrency("USD")
	if err != nil {
		t.Fatalf("ParseCurrency() error = %v", err)
	}
	money, err := ledgerdomain.NewMoney(amount, currency)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	transfer, err := transferdomain.NewTransfer(transferdomain.NewTransferParams{
		ID:                   f.newUUID(t),
		IdempotencyKey:       "transaction-worker-integration",
		RequesterID:          requesterID,
		SourceAccountID:      sourceID,
		DestinationAccountID: destinationID,
		Amount:               money,
		RequestedAt:          time.Date(2026, time.September, 12, 10, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("NewTransfer() error = %v", err)
	}
	pending, err := transferdomain.NewPendingLifecycle(transfer, "risk-v1")
	if err != nil {
		t.Fatalf("NewPendingLifecycle() error = %v", err)
	}
	repository, err := transferpostgres.NewRepository(f.pool)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	if _, err := repository.CreatePending(t.Context(), pending); err != nil {
		t.Fatalf("CreatePending() error = %v", err)
	}
	f.transferIDs = append(f.transferIDs, transfer.ID())
	approved, err := pending.Approve()
	if err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	if _, err := f.pool.Exec(t.Context(), `UPDATE transfers SET status = 'approved', state_version = 2 WHERE id = $1`, transfer.ID()); err != nil {
		t.Fatalf("setting approved lifecycle: %v", err)
	}

	policy, err := riskdomain.NewPolicy(riskdomain.PolicyParams{Version: "risk-v1", Thresholds: []riskdomain.Threshold{{Currency: currency, ReviewAmountMinor: 100, DeclineAmountMinor: 200}}})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	evaluation, err := policy.Evaluate(riskdomain.Input{Amount: money})
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	draft, err := transferevents.RiskAssessed(approved, evaluation, time.Date(2026, time.September, 12, 11, 0, 0, 0, time.UTC), f.newUUID(t))
	if err != nil {
		t.Fatalf("RiskAssessed() error = %v", err)
	}
	envelope, err := event.NewEnvelope(f.newUUID(t), draft)
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}
	f.eventIDs = append(f.eventIDs, envelope.EventID())
	return approved, envelope
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

func (f *integrationFixture) assertLifecycle(t testing.TB, transferID, wantStatus string, wantVersion int64, wantReason string) {
	t.Helper()
	var status, reason string
	var version int64
	if err := f.pool.QueryRow(context.Background(), `SELECT status, state_version, COALESCE(failure_reason, '') FROM transfers WHERE id = $1`, transferID).Scan(&status, &version, &reason); err != nil {
		t.Fatalf("selecting lifecycle: %v", err)
	}
	if status != wantStatus || version != wantVersion || reason != wantReason {
		t.Errorf("transfer lifecycle = (%q, %d, %q), want (%q, %d, %q)", status, version, reason, wantStatus, wantVersion, wantReason)
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
		if _, err := f.pool.Exec(ctx, `DELETE FROM transfers WHERE id = ANY($1::uuid[])`, f.transferIDs); err != nil {
			t.Errorf("cleaning transfers: %v", err)
		}
		if _, err := f.pool.Exec(ctx, `DELETE FROM postings WHERE journal_entry_id = ANY($1::uuid[])`, f.transferIDs); err != nil {
			t.Errorf("cleaning postings: %v", err)
		}
		if _, err := f.pool.Exec(ctx, `DELETE FROM journal_entries WHERE id = ANY($1::uuid[])`, f.transferIDs); err != nil {
			t.Errorf("cleaning journal entries: %v", err)
		}
	}
	if len(f.accountIDs) > 0 {
		if _, err := f.pool.Exec(ctx, `DELETE FROM account_balances WHERE account_id = ANY($1::uuid[])`, f.accountIDs); err != nil {
			t.Errorf("cleaning balances: %v", err)
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
