//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	ledgerpostgres "github.com/krav01/intelligent-wallet-ledger/internal/ledger/postgres"
)

func TestPostTxUsesCallerTransaction(t *testing.T) {
	fixture := newFixture(t)
	systemAccount := fixture.createAccount(t, "USD", "system")
	customerAccount := fixture.createAccount(t, "USD", "customer")
	entryID := fixture.newUUID(t)
	entry := mustEntry(t, entryID, []domain.Posting{
		mustPosting(t, systemAccount, -100, "USD"),
		mustPosting(t, customerAccount, 100, "USD"),
	})
	fixture.trackEntry(entryID)

	tx, err := fixture.pool.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		t.Fatalf("BeginTx() error = %v", err)
	}
	if err := ledgerpostgres.PostTx(t.Context(), tx, entry); err != nil {
		t.Fatalf("PostTx() error = %v", err)
	}
	if _, err := tx.Exec(t.Context(), "SELECT 1"); err != nil {
		t.Fatalf("caller transaction was closed: %v", err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	fixture.assertEntryCount(t, entryID, 0)
	fixture.assertBalance(t, systemAccount, 0, 0)
	fixture.assertBalance(t, customerAccount, 0, 0)
}

func TestRepositoryReconcileReportsOnlyDiscrepantSnapshots(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	systemAccount := fixture.createAccount(t, "USD", "system")
	customerAccount := fixture.createAccount(t, "USD", "customer")
	entryID := fixture.newUUID(t)
	fixture.trackEntry(entryID)
	entry := mustEntry(t, entryID, []domain.Posting{
		mustPosting(t, systemAccount, -100, "USD"),
		mustPosting(t, customerAccount, 100, "USD"),
	})
	if err := repository.Post(t.Context(), entry); err != nil {
		t.Fatalf("Repository.Post() error = %v", err)
	}
	discrepancies, err := repository.Reconcile(t.Context())
	if err != nil {
		t.Fatalf("Repository.Reconcile() error = %v", err)
	}
	if len(discrepancies) != 0 {
		t.Fatalf("Reconcile() = %+v, want no discrepancies", discrepancies)
	}
	if _, err := fixture.pool.Exec(t.Context(), `UPDATE account_balances SET balance_minor = -99 WHERE account_id = $1`, systemAccount); err != nil {
		t.Fatalf("corrupting test snapshot: %v", err)
	}
	discrepancies, err = repository.Reconcile(t.Context())
	if err != nil {
		t.Fatalf("Repository.Reconcile(corrupt) error = %v", err)
	}
	if len(discrepancies) != 1 || discrepancies[0].AccountID != systemAccount || discrepancies[0].Currency != "USD" || discrepancies[0].LedgerMinor != "-100" || discrepancies[0].SnapshotMinor != "-99" {
		t.Errorf("Reconcile(corrupt) = %+v, want system-account discrepancy", discrepancies)
	}
}

func TestRepositoryPostGetAndReverse(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	systemAccount := fixture.createAccount(t, "USD", "system")
	customerAccount := fixture.createAccount(t, "USD", "customer")
	entryID := fixture.newUUID(t)
	entry := mustEntry(t, entryID, []domain.Posting{
		mustPosting(t, systemAccount, -100, "USD"),
		mustPosting(t, customerAccount, 100, "USD"),
	})
	fixture.trackEntry(entryID)

	if err := repository.Post(t.Context(), entry); err != nil {
		t.Fatalf("Repository.Post() error = %v", err)
	}
	fixture.assertBalance(t, systemAccount, -100, 1)
	fixture.assertBalance(t, customerAccount, 100, 1)

	got, err := repository.Get(t.Context(), entryID)
	if err != nil {
		t.Fatalf("Repository.Get() error = %v", err)
	}
	assertEntryEqual(t, got, entry)
	if err := repository.Post(t.Context(), entry); !errors.Is(err, ledgerpostgres.ErrAlreadyExists) {
		t.Fatalf("Repository.Post(duplicate) error = %v, want ErrAlreadyExists", err)
	}
	fixture.assertBalance(t, systemAccount, -100, 1)
	fixture.assertBalance(t, customerAccount, 100, 1)

	fakeSource := mustEntry(t, entryID, []domain.Posting{
		mustPosting(t, systemAccount, -50, "USD"),
		mustPosting(t, customerAccount, 50, "USD"),
	})
	fakeReversalID := fixture.newUUID(t)
	fakeReversal, err := fakeSource.Reverse(fakeReversalID, databaseTime())
	if err != nil {
		t.Fatalf("JournalEntry.Reverse() error = %v", err)
	}
	fixture.trackEntry(fakeReversalID)
	if err := repository.Post(t.Context(), fakeReversal); !errors.Is(err, ledgerpostgres.ErrInvalidReversal) {
		t.Fatalf("Repository.Post(fake reversal) error = %v, want ErrInvalidReversal", err)
	}

	reversalID := fixture.newUUID(t)
	reversal, err := entry.Reverse(reversalID, databaseTime())
	if err != nil {
		t.Fatalf("JournalEntry.Reverse() error = %v", err)
	}
	fixture.trackEntry(reversalID)
	if err := repository.Post(t.Context(), reversal); err != nil {
		t.Fatalf("Repository.Post(reversal) error = %v", err)
	}
	fixture.assertBalance(t, systemAccount, 0, 2)
	fixture.assertBalance(t, customerAccount, 0, 2)

	gotReversal, err := repository.Get(t.Context(), reversalID)
	if err != nil {
		t.Fatalf("Repository.Get(reversal) error = %v", err)
	}
	assertEntryEqual(t, gotReversal, reversal)

	secondReversalID := fixture.newUUID(t)
	secondReversal, err := entry.Reverse(secondReversalID, databaseTime())
	if err != nil {
		t.Fatalf("JournalEntry.Reverse() error = %v", err)
	}
	fixture.trackEntry(secondReversalID)
	if err := repository.Post(t.Context(), secondReversal); !errors.Is(err, ledgerpostgres.ErrAlreadyReversed) {
		t.Fatalf("Repository.Post(second reversal) error = %v, want ErrAlreadyReversed", err)
	}
	fixture.assertEntryCount(t, secondReversalID, 0)
	fixture.assertBalance(t, systemAccount, 0, 2)
	fixture.assertBalance(t, customerAccount, 0, 2)
}

func TestRepositoryAggregatesDuplicateAccountPostings(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	customerAccount := fixture.createAccount(t, "USD", "customer")
	systemAccount := fixture.createAccount(t, "USD", "system")
	entryID := fixture.newUUID(t)
	entry := mustEntry(t, entryID, []domain.Posting{
		mustPosting(t, customerAccount, 100, "USD"),
		mustPosting(t, customerAccount, -40, "USD"),
		mustPosting(t, systemAccount, -60, "USD"),
	})
	fixture.trackEntry(entryID)

	if err := repository.Post(t.Context(), entry); err != nil {
		t.Fatalf("Repository.Post() error = %v", err)
	}
	fixture.assertBalance(t, customerAccount, 60, 1)
	fixture.assertBalance(t, systemAccount, -60, 1)
	fixture.assertPostingCount(t, entryID, 3)
}

func TestRepositoryUsesAggregateBalanceEffects(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	customerAccount := fixture.createAccount(t, "USD", "customer")
	systemAccountA := fixture.createAccount(t, "USD", "system")
	systemAccountB := fixture.createAccount(t, "USD", "system")
	entryID := fixture.newUUID(t)
	entry := mustEntry(t, entryID, []domain.Posting{
		mustPosting(t, customerAccount, -100, "USD"),
		mustPosting(t, customerAccount, 100, "USD"),
		mustPosting(t, systemAccountA, 1, "USD"),
		mustPosting(t, systemAccountB, -1, "USD"),
	})
	fixture.trackEntry(entryID)

	if err := repository.Post(t.Context(), entry); err != nil {
		t.Fatalf("Repository.Post() error = %v", err)
	}
	fixture.assertBalance(t, customerAccount, 0, 1)
	fixture.assertBalance(t, systemAccountA, 1, 1)
	fixture.assertBalance(t, systemAccountB, -1, 1)
}

func TestRepositoryRejectsInvalidAccountsAtomically(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	customerAccount := fixture.createAccount(t, "USD", "customer")
	systemAccount := fixture.createAccount(t, "USD", "system")

	tests := []struct {
		name     string
		postings []domain.Posting
		want     error
	}{
		{
			name: "insufficient funds",
			postings: []domain.Posting{
				mustPosting(t, customerAccount, -1, "USD"),
				mustPosting(t, systemAccount, 1, "USD"),
			},
			want: ledgerpostgres.ErrInsufficientFunds,
		},
		{
			name: "currency mismatch",
			postings: []domain.Posting{
				mustPosting(t, customerAccount, 1, "EUR"),
				mustPosting(t, systemAccount, -1, "EUR"),
			},
			want: ledgerpostgres.ErrCurrencyMismatch,
		},
		{
			name: "missing account",
			postings: []domain.Posting{
				mustPosting(t, fixture.newUUID(t), 1, "USD"),
				mustPosting(t, systemAccount, -1, "USD"),
			},
			want: ledgerpostgres.ErrNotFound,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entryID := fixture.newUUID(t)
			fixture.trackEntry(entryID)
			entry := mustEntry(t, entryID, test.postings)

			err := repository.Post(t.Context(), entry)
			if !errors.Is(err, test.want) {
				t.Fatalf("Repository.Post() error = %v, want %v", err, test.want)
			}
			fixture.assertEntryCount(t, entryID, 0)
			fixture.assertBalance(t, customerAccount, 0, 0)
			fixture.assertBalance(t, systemAccount, 0, 0)
		})
	}
}

func TestRepositoryGetRejectsUnbalancedStoredEntry(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	systemAccount := fixture.createAccount(t, "USD", "system")
	entryID := fixture.newUUID(t)
	fixture.trackEntry(entryID)

	if _, err := fixture.pool.Exec(
		t.Context(),
		"INSERT INTO journal_entries (id, entry_type, recorded_at) VALUES ($1, 'standard', CURRENT_TIMESTAMP)",
		entryID,
	); err != nil {
		t.Fatalf("inserting corrupt journal entry: %v", err)
	}
	for position, amount := range []int64{100, -50} {
		if _, err := fixture.pool.Exec(
			t.Context(),
			"INSERT INTO postings (journal_entry_id, position, account_id, currency, amount_minor) VALUES ($1, $2, $3, 'USD', $4)",
			entryID,
			position,
			systemAccount,
			amount,
		); err != nil {
			t.Fatalf("inserting corrupt posting: %v", err)
		}
	}

	_, err := repository.Get(t.Context(), entryID)
	if !errors.Is(err, ledgerpostgres.ErrCorruptData) {
		t.Fatalf("Repository.Get() error = %v, want ErrCorruptData", err)
	}
}

func TestRepositoryRejectsSnapshotOverflowAtomically(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	systemAccountA := fixture.createAccount(t, "USD", "system")
	systemAccountB := fixture.createAccount(t, "USD", "system")
	if _, err := fixture.pool.Exec(
		t.Context(),
		"UPDATE account_balances SET balance_minor = $1 WHERE account_id = $2",
		int64(math.MaxInt64),
		systemAccountA,
	); err != nil {
		t.Fatalf("preparing maximum balance: %v", err)
	}

	entryID := fixture.newUUID(t)
	fixture.trackEntry(entryID)
	entry := mustEntry(t, entryID, []domain.Posting{
		mustPosting(t, systemAccountA, 1, "USD"),
		mustPosting(t, systemAccountB, -1, "USD"),
	})
	if err := repository.Post(t.Context(), entry); !errors.Is(err, ledgerpostgres.ErrAmountOverflow) {
		t.Fatalf("Repository.Post() error = %v, want ErrAmountOverflow", err)
	}
	fixture.assertEntryCount(t, entryID, 0)
	fixture.assertBalance(t, systemAccountA, math.MaxInt64, 0)
	fixture.assertBalance(t, systemAccountB, 0, 0)
}

func TestRepositoryRejectsSnapshotVersionOverflowAtomically(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	customerAccount := fixture.createAccount(t, "USD", "customer")
	systemAccount := fixture.createAccount(t, "USD", "system")
	if _, err := fixture.pool.Exec(
		t.Context(),
		"UPDATE account_balances SET version = $1 WHERE account_id = $2",
		int64(math.MaxInt64),
		customerAccount,
	); err != nil {
		t.Fatalf("preparing maximum version: %v", err)
	}

	entryID := fixture.newUUID(t)
	fixture.trackEntry(entryID)
	entry := mustEntry(t, entryID, []domain.Posting{
		mustPosting(t, customerAccount, 1, "USD"),
		mustPosting(t, systemAccount, -1, "USD"),
	})
	if err := repository.Post(t.Context(), entry); !errors.Is(err, ledgerpostgres.ErrAmountOverflow) {
		t.Fatalf("Repository.Post() error = %v, want ErrAmountOverflow", err)
	}
	fixture.assertEntryCount(t, entryID, 0)
	fixture.assertBalance(t, customerAccount, 0, math.MaxInt64)
	fixture.assertBalance(t, systemAccount, 0, 0)
}

func TestRepositoryConcurrentWithdrawalsDoNotOverdraw(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	customerAccount := fixture.createAccount(t, "USD", "customer")
	systemAccount := fixture.createAccount(t, "USD", "system")

	fundingID := fixture.newUUID(t)
	fixture.trackEntry(fundingID)
	funding := mustEntry(t, fundingID, []domain.Posting{
		mustPosting(t, customerAccount, 100, "USD"),
		mustPosting(t, systemAccount, -100, "USD"),
	})
	if err := repository.Post(t.Context(), funding); err != nil {
		t.Fatalf("Repository.Post(funding) error = %v", err)
	}

	entries := make([]domain.JournalEntry, 0, 2)
	for range 2 {
		entryID := fixture.newUUID(t)
		fixture.trackEntry(entryID)
		entries = append(entries, mustEntry(t, entryID, []domain.Posting{
			mustPosting(t, customerAccount, -80, "USD"),
			mustPosting(t, systemAccount, 80, "USD"),
		}))
	}

	start := make(chan struct{})
	errorsByEntry := make([]error, len(entries))
	var waitGroup sync.WaitGroup
	for index := range entries {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			errorsByEntry[index] = repository.Post(t.Context(), entries[index])
		}()
	}
	close(start)
	waitGroup.Wait()

	succeeded := 0
	insufficient := 0
	for _, err := range errorsByEntry {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ledgerpostgres.ErrInsufficientFunds):
			insufficient++
		default:
			t.Fatalf("concurrent Repository.Post() error = %v", err)
		}
	}
	if succeeded != 1 || insufficient != 1 {
		t.Fatalf("concurrent results: succeeded = %d, insufficient = %d, want 1 and 1", succeeded, insufficient)
	}
	fixture.assertBalance(t, customerAccount, 20, 2)
	fixture.assertBalance(t, systemAccount, -20, 2)
}

func TestRepositoryConcurrentOppositePostingOrderDoesNotDeadlock(t *testing.T) {
	fixture := newFixture(t)
	repository := mustRepository(t, fixture.pool)
	accountA := fixture.createAccount(t, "USD", "system")
	accountB := fixture.createAccount(t, "USD", "system")

	entryAID := fixture.newUUID(t)
	entryBID := fixture.newUUID(t)
	fixture.trackEntry(entryAID)
	fixture.trackEntry(entryBID)
	entries := []domain.JournalEntry{
		mustEntry(t, entryAID, []domain.Posting{
			mustPosting(t, accountA, 10, "USD"),
			mustPosting(t, accountB, -10, "USD"),
		}),
		mustEntry(t, entryBID, []domain.Posting{
			mustPosting(t, accountB, 20, "USD"),
			mustPosting(t, accountA, -20, "USD"),
		}),
	}

	start := make(chan struct{})
	errorsByEntry := make([]error, len(entries))
	var waitGroup sync.WaitGroup
	for index := range entries {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			errorsByEntry[index] = repository.Post(t.Context(), entries[index])
		}()
	}
	close(start)
	waitGroup.Wait()

	for _, err := range errorsByEntry {
		if err != nil {
			t.Fatalf("concurrent Repository.Post() error = %v", err)
		}
	}
	fixture.assertBalance(t, accountA, -10, 2)
	fixture.assertBalance(t, accountB, 10, 2)
}

type fixture struct {
	pool      *pgxpool.Pool
	entryIDs  []string
	walletIDs []string
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
	if err := pool.Ping(t.Context()); err != nil {
		pool.Close()
		t.Fatalf("PostgreSQL ping error = %v", err)
	}

	created := &fixture{
		pool:      pool,
		entryIDs:  []string{},
		walletIDs: []string{},
	}
	t.Cleanup(func() {
		created.cleanup(t)
		pool.Close()
	})

	return created
}

func mustRepository(t testing.TB, pool *pgxpool.Pool) *ledgerpostgres.Repository {
	t.Helper()

	repository, err := ledgerpostgres.NewRepository(pool)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	return repository
}

func (f *fixture) createAccount(t testing.TB, currencyCode, accountType string) string {
	t.Helper()

	const query = `
WITH inserted_wallet AS (
    INSERT INTO wallets (owner_id)
    VALUES (gen_random_uuid())
    RETURNING id
), inserted_account AS (
    INSERT INTO accounts (wallet_id, currency, account_type)
    SELECT id, $1, $2
    FROM inserted_wallet
    RETURNING id, wallet_id, currency, account_type
), inserted_balance AS (
    INSERT INTO account_balances (account_id, currency, account_type)
    SELECT id, currency, account_type
    FROM inserted_account
    RETURNING account_id
)
SELECT account.id::text, account.wallet_id::text
FROM inserted_account AS account
JOIN inserted_balance AS balance ON balance.account_id = account.id`

	var accountID string
	var walletID string
	if err := f.pool.QueryRow(context.Background(), query, currencyCode, accountType).Scan(
		&accountID,
		&walletID,
	); err != nil {
		t.Fatalf("creating %s account: %v", accountType, err)
	}
	f.walletIDs = append(f.walletIDs, walletID)

	return accountID
}

func (f *fixture) newUUID(t testing.TB) string {
	t.Helper()

	var id string
	if err := f.pool.QueryRow(context.Background(), "SELECT gen_random_uuid()::text").Scan(&id); err != nil {
		t.Fatalf("generating UUID: %v", err)
	}

	return id
}

func (f *fixture) trackEntry(entryID string) {
	f.entryIDs = append(f.entryIDs, entryID)
}

func (f *fixture) assertBalance(t testing.TB, accountID string, wantBalance, wantVersion int64) {
	t.Helper()

	var balance int64
	var version int64
	if err := f.pool.QueryRow(
		context.Background(),
		"SELECT balance_minor, version FROM account_balances WHERE account_id = $1",
		accountID,
	).Scan(&balance, &version); err != nil {
		t.Fatalf("selecting account balance: %v", err)
	}
	if balance != wantBalance || version != wantVersion {
		t.Errorf(
			"account snapshot = (balance %d, version %d), want (%d, %d)",
			balance,
			version,
			wantBalance,
			wantVersion,
		)
	}
}

func (f *fixture) assertEntryCount(t testing.TB, entryID string, want int) {
	t.Helper()

	var count int
	if err := f.pool.QueryRow(
		context.Background(),
		"SELECT COUNT(*) FROM journal_entries WHERE id = $1",
		entryID,
	).Scan(&count); err != nil {
		t.Fatalf("counting journal entry: %v", err)
	}
	if count != want {
		t.Errorf("journal entry count = %d, want %d", count, want)
	}
}

func (f *fixture) assertPostingCount(t testing.TB, entryID string, want int) {
	t.Helper()

	var count int
	if err := f.pool.QueryRow(
		context.Background(),
		"SELECT COUNT(*) FROM postings WHERE journal_entry_id = $1",
		entryID,
	).Scan(&count); err != nil {
		t.Fatalf("counting journal postings: %v", err)
	}
	if count != want {
		t.Errorf("journal posting count = %d, want %d", count, want)
	}
}

func (f *fixture) cleanup(t testing.TB) {
	t.Helper()

	if len(f.entryIDs) > 0 {
		if _, err := f.pool.Exec(
			context.Background(),
			"DELETE FROM postings WHERE journal_entry_id = ANY($1::uuid[])",
			f.entryIDs,
		); err != nil {
			t.Errorf("cleaning postings: %v", err)
		}
		if _, err := f.pool.Exec(
			context.Background(),
			"DELETE FROM journal_entries WHERE id = ANY($1::uuid[]) AND reverses_entry_id IS NOT NULL",
			f.entryIDs,
		); err != nil {
			t.Errorf("cleaning reversal entries: %v", err)
		}
		if _, err := f.pool.Exec(
			context.Background(),
			"DELETE FROM journal_entries WHERE id = ANY($1::uuid[])",
			f.entryIDs,
		); err != nil {
			t.Errorf("cleaning journal entries: %v", err)
		}
	}
	if len(f.walletIDs) > 0 {
		if _, err := f.pool.Exec(
			context.Background(),
			"DELETE FROM wallets WHERE id = ANY($1::uuid[])",
			f.walletIDs,
		); err != nil {
			t.Errorf("cleaning wallets: %v", err)
		}
	}
}

func mustEntry(t testing.TB, id string, postings []domain.Posting) domain.JournalEntry {
	t.Helper()

	entry, err := domain.NewJournalEntry(domain.NewJournalEntryParams{
		ID:         id,
		Postings:   postings,
		RecordedAt: databaseTime(),
	})
	if err != nil {
		t.Fatalf("NewJournalEntry() error = %v", err)
	}

	return entry
}

func databaseTime() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond)
}

func mustPosting(
	t testing.TB,
	accountID string,
	minorUnits int64,
	currencyCode string,
) domain.Posting {
	t.Helper()

	currency, err := domain.ParseCurrency(currencyCode)
	if err != nil {
		t.Fatalf("ParseCurrency(%q) error = %v", currencyCode, err)
	}
	amount, err := domain.NewMoney(minorUnits, currency)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	posting, err := domain.NewPosting(accountID, amount)
	if err != nil {
		t.Fatalf("NewPosting() error = %v", err)
	}

	return posting
}

func assertEntryEqual(t testing.TB, got, want domain.JournalEntry) {
	t.Helper()

	if got.ID() != want.ID() ||
		got.Type() != want.Type() ||
		got.ReversesEntryID() != want.ReversesEntryID() ||
		!got.RecordedAt().Equal(want.RecordedAt()) {
		t.Errorf("journal entry header = %+v, want %+v", got, want)
	}
	gotPostings := got.Postings()
	wantPostings := want.Postings()
	if len(gotPostings) != len(wantPostings) {
		t.Fatalf("journal posting count = %d, want %d", len(gotPostings), len(wantPostings))
	}
	for index := range gotPostings {
		if gotPostings[index].AccountID() != wantPostings[index].AccountID() ||
			gotPostings[index].Amount() != wantPostings[index].Amount() {
			t.Errorf("journal posting %d = %+v, want %+v", index, gotPostings[index], wantPostings[index])
		}
	}
}

func TestMigrationRejectsIrreversiblePosting(t *testing.T) {
	fixture := newFixture(t)
	systemAccount := fixture.createAccount(t, "USD", "system")
	entryID := fixture.newUUID(t)
	fixture.trackEntry(entryID)

	if _, err := fixture.pool.Exec(
		t.Context(),
		"INSERT INTO journal_entries (id, entry_type, recorded_at) VALUES ($1, 'standard', CURRENT_TIMESTAMP)",
		entryID,
	); err != nil {
		t.Fatalf("inserting journal entry: %v", err)
	}
	_, err := fixture.pool.Exec(
		t.Context(),
		"INSERT INTO postings (journal_entry_id, position, account_id, currency, amount_minor) VALUES ($1, 0, $2, 'USD', $3)",
		entryID,
		systemAccount,
		int64(math.MinInt64),
	)
	if err == nil {
		t.Fatal("irreversible posting insert error = nil, want constraint violation")
	}
}
