//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	ledgerpostgres "github.com/krav01/intelligent-wallet-ledger/internal/ledger/postgres"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
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

	replay := fixture.transfer(t, ownerID, sourceID, destinationID, "request-1", 60)
	replayed, err := repository.Create(t.Context(), replay)
	if err != nil {
		t.Fatalf("Repository.Create(replay) error = %v", err)
	}
	assertTransferEqual(t, replayed, original)
	fixture.assertBalance(t, sourceID, 40, 2)
	fixture.assertBalance(t, destinationID, 60, 1)
	fixture.assertTransferCount(t, ownerID, "request-1", 1)
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

	fixture.fund(t, sourceID, 25)
	created, err := repository.Create(t.Context(), request)
	if err != nil {
		t.Fatalf("Repository.Create(retry) error = %v", err)
	}
	assertTransferEqual(t, created, request)
	fixture.assertBalance(t, sourceID, 0, 2)
	fixture.assertBalance(t, destinationID, 25, 1)
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

func (f *fixture) cleanup(t testing.TB) {
	t.Helper()
	ctx := context.Background()
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
