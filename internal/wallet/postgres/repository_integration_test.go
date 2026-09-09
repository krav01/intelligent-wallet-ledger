//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	"github.com/krav01/intelligent-wallet-ledger/internal/wallet"
	walletpostgres "github.com/krav01/intelligent-wallet-ledger/internal/wallet/postgres"
)

func TestRepositoryCreateAndGet(t *testing.T) {
	pool := openTestPool(t)
	repository := mustRepository(t, pool)
	ownerID := newUUID(t, pool)
	eur := mustCurrency(t, "EUR")
	usd := mustCurrency(t, "USD")

	created, err := repository.Create(t.Context(), ownerID, []ledgerdomain.Currency{usd, eur})
	if err != nil {
		t.Fatalf("Repository.Create() error = %v", err)
	}
	cleanupWallet(t, pool, created.ID)

	if created.OwnerID != ownerID {
		t.Errorf("created owner ID = %q, want %q", created.OwnerID, ownerID)
	}
	assertZeroAccounts(t, created.Accounts, []string{"EUR", "USD"})

	got, err := repository.Get(t.Context(), created.ID)
	if err != nil {
		t.Fatalf("Repository.Get() error = %v", err)
	}
	if got.ID != created.ID || got.OwnerID != ownerID || got.Status != created.Status {
		t.Errorf("Repository.Get() = %+v, want wallet identity %+v", got, created)
	}
	assertZeroAccounts(t, got.Accounts, []string{"EUR", "USD"})
}

func TestRepositoryGetReturnsNotFound(t *testing.T) {
	pool := openTestPool(t)
	repository := mustRepository(t, pool)

	_, err := repository.Get(t.Context(), newUUID(t, pool))
	if !errors.Is(err, walletpostgres.ErrNotFound) {
		t.Fatalf("Repository.Get() error = %v, want ErrNotFound", err)
	}
}

func TestAccountBalanceConstraints(t *testing.T) {
	pool := openTestPool(t)
	repository := mustRepository(t, pool)
	created, err := repository.Create(
		t.Context(),
		newUUID(t, pool),
		[]ledgerdomain.Currency{mustCurrency(t, "USD")},
	)
	if err != nil {
		t.Fatalf("Repository.Create() error = %v", err)
	}
	cleanupWallet(t, pool, created.ID)

	_, err = pool.Exec(
		t.Context(),
		"UPDATE account_balances SET balance_minor = $1 WHERE account_id = $2",
		-1,
		created.Accounts[0].ID,
	)
	if err == nil {
		t.Fatal("negative account balance update error = nil, want constraint violation")
	}
}

func openTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for integration tests")
	}

	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(t.Context()); err != nil {
		t.Fatalf("PostgreSQL ping error = %v", err)
	}

	return pool
}

func mustRepository(t testing.TB, pool *pgxpool.Pool) *walletpostgres.Repository {
	t.Helper()

	repository, err := walletpostgres.NewRepository(pool)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	return repository
}

func newUUID(t testing.TB, pool *pgxpool.Pool) string {
	t.Helper()

	var id string
	if err := pool.QueryRow(context.Background(), "SELECT gen_random_uuid()::text").Scan(&id); err != nil {
		t.Fatalf("generating UUID: %v", err)
	}

	return id
}

func cleanupWallet(t testing.TB, pool *pgxpool.Pool, walletID string) {
	t.Helper()

	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "DELETE FROM wallets WHERE id = $1", walletID); err != nil {
			t.Errorf("cleaning wallet %s: %v", walletID, err)
		}
	})
}

func mustCurrency(t testing.TB, code string) ledgerdomain.Currency {
	t.Helper()

	currency, err := ledgerdomain.ParseCurrency(code)
	if err != nil {
		t.Fatalf("ParseCurrency(%q) error = %v", code, err)
	}

	return currency
}

func assertZeroAccounts(t testing.TB, accounts []wallet.Account, wantCurrencies []string) {
	t.Helper()

	if len(accounts) != len(wantCurrencies) {
		t.Fatalf("account count = %d, want %d", len(accounts), len(wantCurrencies))
	}
	for index, account := range accounts {
		if got := account.Balance.Currency().String(); got != wantCurrencies[index] {
			t.Errorf("account %d currency = %q, want %q", index, got, wantCurrencies[index])
		}
		if got := account.Balance.MinorUnits(); got != 0 {
			t.Errorf("account %d balance = %d, want 0", index, got)
		}
		if account.BalanceVersion != 0 {
			t.Errorf("account %d balance version = %d, want 0", index, account.BalanceVersion)
		}
	}
}
