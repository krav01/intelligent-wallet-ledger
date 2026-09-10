//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	inboxpostgres "github.com/krav01/intelligent-wallet-ledger/internal/inbox/postgres"
)

func TestReserveTxDeduplicatesPerConsumerAndRollsBack(t *testing.T) {
	fixture := newFixture(t)
	envelope := fixture.envelope(t)
	fixture.track(envelope.EventID())

	tx := fixture.begin(t)
	reserved, err := inboxpostgres.ReserveTx(t.Context(), tx, "risk-worker.v1", envelope)
	if err != nil || !reserved {
		t.Fatalf("ReserveTx() = (%t, %v), want (true, nil)", reserved, err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	tx = fixture.begin(t)
	reserved, err = inboxpostgres.ReserveTx(t.Context(), tx, "risk-worker.v1", envelope)
	if err != nil || reserved {
		t.Fatalf("ReserveTx(duplicate) = (%t, %v), want (false, nil)", reserved, err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("Commit(duplicate) error = %v", err)
	}

	tx = fixture.begin(t)
	reserved, err = inboxpostgres.ReserveTx(t.Context(), tx, "projection-worker.v1", envelope)
	if err != nil || !reserved {
		t.Fatalf("ReserveTx(other consumer) = (%t, %v), want (true, nil)", reserved, err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	tx = fixture.begin(t)
	reserved, err = inboxpostgres.ReserveTx(t.Context(), tx, "projection-worker.v1", envelope)
	if err != nil || !reserved {
		t.Fatalf("ReserveTx(after rollback) = (%t, %v), want (true, nil)", reserved, err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("Commit(after rollback) error = %v", err)
	}
	fixture.assertCount(t, envelope.EventID(), 2)
}

func TestReserveTxAcceptsEventTypeWithUnderscore(t *testing.T) {
	fixture := newFixture(t)
	envelope := fixture.envelopeWithEventType(t, "transfer.risk_assessed")
	fixture.track(envelope.EventID())

	tx := fixture.begin(t)
	reserved, err := inboxpostgres.ReserveTx(t.Context(), tx, "risk-worker.v1", envelope)
	if err != nil || !reserved {
		t.Fatalf("ReserveTx() = (%t, %v), want (true, nil)", reserved, err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	fixture.assertCount(t, envelope.EventID(), 1)
}

func TestReserveTxConcurrentDuplicatesCommitOnce(t *testing.T) {
	fixture := newFixture(t)
	envelope := fixture.envelope(t)
	fixture.track(envelope.EventID())
	ownerID := fixture.newUUID(t)
	fixture.ownerIDs = append(fixture.ownerIDs, ownerID)

	const workers = 8
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	results := make(chan reserveResult, workers)
	for range workers {
		go func() {
			<-start
			tx, err := fixture.pool.Begin(ctx)
			if err != nil {
				results <- reserveResult{err: err}
				return
			}
			reserved, err := inboxpostgres.ReserveTx(
				ctx,
				tx,
				"risk-worker.v1",
				envelope,
			)
			if err != nil {
				rollbackErr := tx.Rollback(ctx)
				results <- reserveResult{err: errors.Join(err, rollbackErr)}
				return
			}
			if reserved {
				if _, err := tx.Exec(ctx, `INSERT INTO wallets (owner_id) VALUES ($1)`, ownerID); err != nil {
					rollbackErr := tx.Rollback(ctx)
					results <- reserveResult{err: errors.Join(err, rollbackErr)}
					return
				}
			}
			if err := tx.Commit(ctx); err != nil {
				results <- reserveResult{err: err}
				return
			}
			results <- reserveResult{reserved: reserved}
		}()
	}
	close(start)

	var reservedCount int
	for range workers {
		result := <-results
		if result.err != nil {
			t.Errorf("concurrent ReserveTx() error = %v", result.err)
		}
		if result.reserved {
			reservedCount++
		}
	}
	if reservedCount != 1 {
		t.Errorf("successful concurrent reservations = %d, want 1", reservedCount)
	}
	fixture.assertCount(t, envelope.EventID(), 1)
	fixture.assertWalletCount(t, ownerID, 1)
}

type reserveResult struct {
	reserved bool
	err      error
}

type fixture struct {
	pool     *pgxpool.Pool
	eventIDs []string
	ownerIDs []string
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
		if len(fixture.eventIDs) > 0 {
			if _, err := pool.Exec(
				context.Background(),
				`DELETE FROM consumer_inbox WHERE event_id = ANY($1::uuid[])`,
				fixture.eventIDs,
			); err != nil {
				t.Errorf("cleaning inbox: %v", err)
			}
		}
		if len(fixture.ownerIDs) > 0 {
			if _, err := pool.Exec(
				context.Background(),
				`DELETE FROM wallets WHERE owner_id = ANY($1::uuid[])`,
				fixture.ownerIDs,
			); err != nil {
				t.Errorf("cleaning side-effect wallets: %v", err)
			}
		}
		pool.Close()
	})
	return fixture
}

func (f *fixture) begin(t testing.TB) pgx.Tx {
	t.Helper()
	tx, err := f.pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	return tx
}

func (f *fixture) envelope(t testing.TB) event.Envelope {
	return f.envelopeWithEventType(t, "transfer.completed")
}

func (f *fixture) envelopeWithEventType(t testing.TB, eventType string) event.Envelope {
	t.Helper()
	eventID := f.newUUID(t)
	aggregateID := f.newUUID(t)
	draft, err := event.NewDraft(event.DraftParams{
		EventType:        eventType,
		EventVersion:     1,
		AggregateType:    "transfer",
		AggregateID:      aggregateID,
		AggregateVersion: 1,
		CorrelationID:    aggregateID,
		OccurredAt:       time.Now().UTC(),
		Payload:          json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("NewDraft() error = %v", err)
	}
	envelope, err := event.NewEnvelope(eventID, draft)
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}
	return envelope
}

func (f *fixture) newUUID(t testing.TB) string {
	t.Helper()
	var id string
	if err := f.pool.QueryRow(context.Background(), `SELECT gen_random_uuid()::text`).Scan(&id); err != nil {
		t.Fatalf("generating UUID: %v", err)
	}
	return id
}

func (f *fixture) track(eventID string) {
	f.eventIDs = append(f.eventIDs, eventID)
}

func (f *fixture) assertCount(t testing.TB, eventID string, want int) {
	t.Helper()
	var got int
	if err := f.pool.QueryRow(
		context.Background(),
		`SELECT count(*) FROM consumer_inbox WHERE event_id = $1`,
		eventID,
	).Scan(&got); err != nil {
		t.Fatalf("counting inbox rows: %v", err)
	}
	if got != want {
		t.Errorf("inbox row count = %d, want %d", got, want)
	}
}

func (f *fixture) assertWalletCount(t testing.TB, ownerID string, want int) {
	t.Helper()
	var got int
	if err := f.pool.QueryRow(
		context.Background(),
		`SELECT count(*) FROM wallets WHERE owner_id = $1`,
		ownerID,
	).Scan(&got); err != nil {
		t.Fatalf("counting side-effect wallets: %v", err)
	}
	if got != want {
		t.Errorf("side-effect wallet count = %d, want %d", got, want)
	}
}
