//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/krav01/intelligent-wallet-ledger/internal/event"
	outboxpostgres "github.com/krav01/intelligent-wallet-ledger/internal/outbox/postgres"
)

func TestAddTxUsesCallerTransactionAndRejectsDuplicateEvent(t *testing.T) {
	fixture := newFixture(t)
	draft := fixture.draft(t)

	tx := fixture.begin(t)
	envelope, err := outboxpostgres.AddTx(t.Context(), tx, draft)
	if err != nil {
		t.Fatalf("AddTx() error = %v", err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	fixture.assertEventCount(t, envelope.EventID(), 0)

	tx = fixture.begin(t)
	envelope, err = outboxpostgres.AddTx(t.Context(), tx, draft)
	if err != nil {
		t.Fatalf("AddTx(commit) error = %v", err)
	}
	fixture.track(envelope.EventID())
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	fixture.assertEventCount(t, envelope.EventID(), 1)

	tx = fixture.begin(t)
	if _, err := outboxpostgres.AddTx(t.Context(), tx, draft); !errors.Is(err, outboxpostgres.ErrAlreadyExists) {
		t.Fatalf("AddTx(duplicate) error = %v, want ErrAlreadyExists", err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatalf("Rollback(duplicate) error = %v", err)
	}
}

func TestAddTxAcceptsPostgreSQLJSONBBoundary(t *testing.T) {
	fixture := newFixture(t)
	draft := fixture.draftWithPayload(
		t,
		json.RawMessage(`{"value":"`+strings.Repeat("x", 65523)+`"}`),
	)
	tx := fixture.begin(t)
	envelope, err := outboxpostgres.AddTx(t.Context(), tx, draft)
	if err != nil {
		if rollbackErr := tx.Rollback(t.Context()); rollbackErr != nil {
			t.Errorf("Rollback() after AddTx error = %v", rollbackErr)
		}
		t.Fatalf("AddTx(boundary payload) error = %v", err)
	}
	fixture.track(envelope.EventID())
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	fixture.assertEventCount(t, envelope.EventID(), 1)
}

func TestAddTxClassifiesPostgreSQLJSONBRejection(t *testing.T) {
	fixture := newFixture(t)
	draft := fixture.draftWithPayload(t, json.RawMessage(`{"value":"\u0000"}`))
	tx := fixture.begin(t)
	if _, err := outboxpostgres.AddTx(t.Context(), tx, draft); !errors.Is(err, outboxpostgres.ErrInvalidArgument) {
		t.Errorf("AddTx(unsupported JSONB Unicode) error = %v, want ErrInvalidArgument", err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
}

func TestRepositoryClaimRecoveryAndFencing(t *testing.T) {
	fixture := newFixture(t)
	repository := fixture.repository(t)
	first := fixture.add(t, fixture.draft(t))
	second := fixture.add(t, fixture.draft(t))
	future := fixture.add(t, fixture.draft(t))
	if _, err := fixture.pool.Exec(
		t.Context(),
		`UPDATE outbox_events SET available_at = CURRENT_TIMESTAMP + INTERVAL '1 hour' WHERE event_id = $1`,
		future.EventID(),
	); err != nil {
		t.Fatalf("delaying event: %v", err)
	}

	claimedA, err := repository.Claim(t.Context(), 1, time.Minute)
	if err != nil {
		t.Fatalf("Claim(A) error = %v", err)
	}
	if len(claimedA) != 1 || claimedA[0].Envelope.EventID() != first.EventID() {
		t.Fatalf("Claim(A) = %+v, want first event", claimedA)
	}
	tokenA := claimedA[0].ClaimToken

	claimedB, err := repository.Claim(t.Context(), 10, time.Minute)
	if err != nil {
		t.Fatalf("Claim(B) error = %v", err)
	}
	if len(claimedB) != 1 || claimedB[0].Envelope.EventID() != second.EventID() {
		t.Fatalf("Claim(B) = %+v, want second event", claimedB)
	}
	tokenB := claimedB[0].ClaimToken
	if err := repository.MarkPublished(t.Context(), first.EventID(), tokenB); !errors.Is(err, outboxpostgres.ErrClaimLost) {
		t.Fatalf("MarkPublished(wrong token) error = %v, want ErrClaimLost", err)
	}

	if _, err := fixture.pool.Exec(
		t.Context(),
		`UPDATE outbox_events SET claimed_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE event_id = $1`,
		first.EventID(),
	); err != nil {
		t.Fatalf("expiring claim: %v", err)
	}
	claimedC, err := repository.Claim(t.Context(), 1, time.Minute)
	if err != nil {
		t.Fatalf("Claim(C) error = %v", err)
	}
	if len(claimedC) != 1 || claimedC[0].Envelope.EventID() != first.EventID() {
		t.Fatalf("Claim(C) = %+v, want recovered first event", claimedC)
	}
	tokenC := claimedC[0].ClaimToken
	if err := repository.MarkFailed(t.Context(), first.EventID(), tokenA, 0, "stale-owner"); !errors.Is(err, outboxpostgres.ErrClaimLost) {
		t.Fatalf("MarkFailed(stale token) error = %v, want ErrClaimLost", err)
	}
	if err := repository.MarkPublished(t.Context(), first.EventID(), tokenC); err != nil {
		t.Fatalf("MarkPublished() error = %v", err)
	}
	if err := repository.MarkPublished(t.Context(), first.EventID(), tokenC); !errors.Is(err, outboxpostgres.ErrClaimLost) {
		t.Fatalf("MarkPublished(repeat) error = %v, want ErrClaimLost", err)
	}

	if err := repository.MarkFailed(t.Context(), second.EventID(), tokenB, 10*time.Second, "broker\ntimeout"); err != nil {
		t.Fatalf("MarkFailed() error = %v", err)
	}
	var attempts int
	var claimedBy *string
	var lastError string
	if err := fixture.pool.QueryRow(
		t.Context(),
		`SELECT publish_attempts, claimed_by::text, last_error FROM outbox_events WHERE event_id = $1`,
		second.EventID(),
	).Scan(&attempts, &claimedBy, &lastError); err != nil {
		t.Fatalf("selecting failed event: %v", err)
	}
	if attempts != 1 || claimedBy != nil || lastError != "broker?timeout" {
		t.Errorf("failed state = (%d, %v, %q), want (1, nil, broker?timeout)", attempts, claimedBy, lastError)
	}

	claimedD, err := repository.Claim(t.Context(), 10, time.Minute)
	if err != nil {
		t.Fatalf("Claim(before retry) error = %v", err)
	}
	if len(claimedD) != 0 {
		t.Fatalf("Claim(before retry) length = %d, want 0", len(claimedD))
	}
	if _, err := fixture.pool.Exec(
		t.Context(),
		`UPDATE outbox_events SET available_at = CURRENT_TIMESTAMP WHERE event_id = $1`,
		second.EventID(),
	); err != nil {
		t.Fatalf("making failed event available: %v", err)
	}
	claimedD, err = repository.Claim(t.Context(), 10, time.Minute)
	if err != nil {
		t.Fatalf("Claim(retry) error = %v", err)
	}
	if len(claimedD) != 1 || claimedD[0].Envelope.EventID() != second.EventID() ||
		claimedD[0].PublishAttempts != 1 {
		t.Fatalf("Claim(retry) = %+v, want failed event with one attempt", claimedD)
	}
}

func TestRepositoryConcurrentClaimsAreDisjoint(t *testing.T) {
	fixture := newFixture(t)
	repository := fixture.repository(t)
	for range 10 {
		fixture.add(t, fixture.draft(t))
	}
	const workers = 2
	results := make(chan []outboxpostgres.ClaimedEvent, workers)
	errorsCh := make(chan error, workers)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			claimed, err := repository.Claim(context.Background(), 5, time.Minute)
			if err != nil {
				errorsCh <- err
				return
			}
			results <- claimed
		}()
	}
	group.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		t.Errorf("Claim() error = %v", err)
	}

	seen := make(map[string]bool, 10)
	for claimed := range results {
		if len(claimed) != 5 {
			t.Errorf("claimed batch length = %d, want 5", len(claimed))
		}
		for _, stored := range claimed {
			if seen[stored.Envelope.EventID()] {
				t.Errorf("event %s was claimed twice", stored.Envelope.EventID())
			}
			seen[stored.Envelope.EventID()] = true
		}
	}
	if len(seen) != 10 {
		t.Errorf("unique claimed events = %d, want 10", len(seen))
	}
}

type fixture struct {
	pool     *pgxpool.Pool
	eventIDs []string
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
				`DELETE FROM outbox_events WHERE event_id = ANY($1::uuid[])`,
				fixture.eventIDs,
			); err != nil {
				t.Errorf("cleaning outbox events: %v", err)
			}
		}
		pool.Close()
	})
	return fixture
}

func (f *fixture) repository(t testing.TB) *outboxpostgres.Repository {
	t.Helper()
	repository, err := outboxpostgres.NewRepository(f.pool)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	return repository
}

func (f *fixture) begin(t testing.TB) pgx.Tx {
	t.Helper()
	tx, err := f.pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	return tx
}

func (f *fixture) add(t testing.TB, draft event.Draft) event.Envelope {
	t.Helper()
	tx := f.begin(t)
	envelope, err := outboxpostgres.AddTx(context.Background(), tx, draft)
	if err != nil {
		if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil {
			t.Errorf("Rollback() after AddTx error = %v", rollbackErr)
		}
		t.Fatalf("AddTx() error = %v", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	f.track(envelope.EventID())
	return envelope
}

func (f *fixture) draft(t testing.TB) event.Draft {
	t.Helper()
	id := f.newUUID(t)
	return f.draftWithIdentity(t, id, json.RawMessage(`{"transfer_id":"`+id+`"}`))
}

func (f *fixture) draftWithPayload(t testing.TB, payload json.RawMessage) event.Draft {
	t.Helper()
	id := f.newUUID(t)
	return f.draftWithIdentity(t, id, payload)
}

func (f *fixture) draftWithIdentity(t testing.TB, id string, payload json.RawMessage) event.Draft {
	t.Helper()
	draft, err := event.NewDraft(event.DraftParams{
		EventType:        "transfer.completed",
		EventVersion:     1,
		AggregateType:    "transfer",
		AggregateID:      id,
		AggregateVersion: 1,
		CorrelationID:    id,
		OccurredAt:       time.Now().UTC().Truncate(time.Microsecond),
		Payload:          payload,
	})
	if err != nil {
		t.Fatalf("NewDraft() error = %v", err)
	}
	return draft
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

func (f *fixture) assertEventCount(t testing.TB, eventID string, want int) {
	t.Helper()
	var got int
	if err := f.pool.QueryRow(
		context.Background(),
		`SELECT count(*) FROM outbox_events WHERE event_id = $1`,
		eventID,
	).Scan(&got); err != nil {
		t.Fatalf("counting events: %v", err)
	}
	if got != want {
		t.Errorf("event count = %d, want %d", got, want)
	}
}
