// Package postgres persists wallets and account balance snapshots in PostgreSQL.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	"github.com/krav01/intelligent-wallet-ledger/internal/wallet"
)

var (
	// ErrInvalidArgument indicates malformed repository input.
	ErrInvalidArgument = errors.New("wallet repository: invalid argument")
	// ErrNotFound indicates that a wallet does not exist.
	ErrNotFound = errors.New("wallet repository: wallet not found")
)

const (
	maxCurrenciesPerWallet = 32

	insertWalletQuery = `
INSERT INTO wallets (owner_id)
VALUES ($1)
RETURNING id::text, owner_id::text, status, created_at, updated_at`

	insertAccountQuery = `
WITH inserted_account AS (
    INSERT INTO accounts (wallet_id, currency)
    VALUES ($1, $2)
    RETURNING id, wallet_id, currency, status, created_at, updated_at
), inserted_balance AS (
    INSERT INTO account_balances (account_id, currency)
    SELECT id, currency
    FROM inserted_account
    RETURNING account_id, currency, balance_minor, version, updated_at
)
SELECT
    account.id::text,
    account.wallet_id::text,
    account.status,
    account.created_at,
    balance.balance_minor,
    balance.version,
    balance.updated_at
FROM inserted_account AS account
JOIN inserted_balance AS balance
  ON balance.account_id = account.id
 AND balance.currency = account.currency`

	selectWalletQuery = `
SELECT id::text, owner_id::text, status, created_at, updated_at
FROM wallets
WHERE id = $1`

	selectAccountsQuery = `
SELECT
    account.id::text,
    account.wallet_id::text,
    account.currency,
    account.status,
    account.created_at,
    balance.balance_minor,
    balance.version,
    balance.updated_at
FROM accounts AS account
JOIN account_balances AS balance
  ON balance.account_id = account.id
 AND balance.currency = account.currency
WHERE account.wallet_id = $1
ORDER BY account.currency`
)

// Repository stores wallets and their balance snapshots.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a PostgreSQL wallet repository.
func NewRepository(pool *pgxpool.Pool) (*Repository, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: pool is required", ErrInvalidArgument)
	}

	return &Repository{pool: pool}, nil
}

// Create atomically creates a wallet, one account per currency, and zero balances.
func (r *Repository) Create(
	ctx context.Context,
	ownerID string,
	currencies []ledgerdomain.Currency,
) (created wallet.Wallet, err error) {
	ownerUUID, err := parseUUID(ownerID)
	if err != nil {
		return wallet.Wallet{}, fmt.Errorf("%w: owner ID: %w", ErrInvalidArgument, err)
	}

	currencyCodes, err := normalizeCurrencies(currencies)
	if err != nil {
		return wallet.Wallet{}, err
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return wallet.Wallet{}, fmt.Errorf("beginning wallet transaction: %w", err)
	}
	defer finishTransaction(ctx, tx, &created, &err)

	created, err = insertWallet(ctx, tx, ownerUUID)
	if err != nil {
		return wallet.Wallet{}, err
	}

	created.Accounts = make([]wallet.Account, 0, len(currencyCodes))
	for _, currencyCode := range currencyCodes {
		account, insertErr := insertAccount(ctx, tx, created.ID, currencyCode)
		if insertErr != nil {
			return wallet.Wallet{}, insertErr
		}

		created.Accounts = append(created.Accounts, account)
	}

	if err := tx.Commit(ctx); err != nil {
		return wallet.Wallet{}, fmt.Errorf("committing wallet transaction: %w", err)
	}

	return created, nil
}

// Get returns a transactionally consistent wallet and account snapshot.
func (r *Repository) Get(ctx context.Context, walletID string) (result wallet.Wallet, err error) {
	walletUUID, err := parseUUID(walletID)
	if err != nil {
		return wallet.Wallet{}, fmt.Errorf("%w: wallet ID: %w", ErrInvalidArgument, err)
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return wallet.Wallet{}, fmt.Errorf("beginning wallet read transaction: %w", err)
	}
	defer finishTransaction(ctx, tx, &result, &err)

	result, err = selectWallet(ctx, tx, walletUUID)
	if err != nil {
		return wallet.Wallet{}, err
	}

	result.Accounts, err = selectAccounts(ctx, tx, walletUUID)
	if err != nil {
		return wallet.Wallet{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return wallet.Wallet{}, fmt.Errorf("committing wallet read transaction: %w", err)
	}

	return result, nil
}

func insertWallet(ctx context.Context, tx pgx.Tx, ownerID pgtype.UUID) (wallet.Wallet, error) {
	var stored wallet.Wallet
	var status string

	err := tx.QueryRow(ctx, insertWalletQuery, ownerID).Scan(
		&stored.ID,
		&stored.OwnerID,
		&status,
		&stored.CreatedAt,
		&stored.UpdatedAt,
	)
	if err != nil {
		return wallet.Wallet{}, fmt.Errorf("inserting wallet: %w", err)
	}

	stored.Status, err = wallet.ParseStatus(status)
	if err != nil {
		return wallet.Wallet{}, fmt.Errorf("decoding wallet status: %w", err)
	}

	return stored, nil
}

func insertAccount(
	ctx context.Context,
	tx pgx.Tx,
	walletID string,
	currencyCode string,
) (wallet.Account, error) {
	var account wallet.Account
	var status string
	var balanceMinor int64

	err := tx.QueryRow(ctx, insertAccountQuery, walletID, currencyCode).Scan(
		&account.ID,
		&account.WalletID,
		&status,
		&account.CreatedAt,
		&balanceMinor,
		&account.BalanceVersion,
		&account.UpdatedAt,
	)
	if err != nil {
		return wallet.Account{}, fmt.Errorf("inserting %s account: %w", currencyCode, err)
	}

	return decodeAccount(account, currencyCode, status, balanceMinor)
}

func selectWallet(ctx context.Context, tx pgx.Tx, walletID pgtype.UUID) (wallet.Wallet, error) {
	var stored wallet.Wallet
	var status string

	err := tx.QueryRow(ctx, selectWalletQuery, walletID).Scan(
		&stored.ID,
		&stored.OwnerID,
		&status,
		&stored.CreatedAt,
		&stored.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return wallet.Wallet{}, ErrNotFound
	}
	if err != nil {
		return wallet.Wallet{}, fmt.Errorf("selecting wallet: %w", err)
	}

	stored.Status, err = wallet.ParseStatus(status)
	if err != nil {
		return wallet.Wallet{}, fmt.Errorf("decoding wallet status: %w", err)
	}

	return stored, nil
}

func selectAccounts(
	ctx context.Context,
	tx pgx.Tx,
	walletID pgtype.UUID,
) ([]wallet.Account, error) {
	rows, err := tx.Query(ctx, selectAccountsQuery, walletID)
	if err != nil {
		return nil, fmt.Errorf("selecting wallet accounts: %w", err)
	}
	defer rows.Close()

	accounts := []wallet.Account{}
	for rows.Next() {
		var account wallet.Account
		var currencyCode string
		var status string
		var balanceMinor int64

		if err := rows.Scan(
			&account.ID,
			&account.WalletID,
			&currencyCode,
			&status,
			&account.CreatedAt,
			&balanceMinor,
			&account.BalanceVersion,
			&account.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scanning wallet account: %w", err)
		}

		account, err = decodeAccount(account, currencyCode, status, balanceMinor)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, account)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating wallet accounts: %w", err)
	}

	return accounts, nil
}

func decodeAccount(
	account wallet.Account,
	currencyCode string,
	statusValue string,
	balanceMinor int64,
) (wallet.Account, error) {
	currency, err := ledgerdomain.ParseCurrency(currencyCode)
	if err != nil {
		return wallet.Account{}, fmt.Errorf("decoding account currency: %w", err)
	}

	account.Status, err = wallet.ParseStatus(statusValue)
	if err != nil {
		return wallet.Account{}, fmt.Errorf("decoding account status: %w", err)
	}

	account.Balance, err = ledgerdomain.NewMoney(balanceMinor, currency)
	if err != nil {
		return wallet.Account{}, fmt.Errorf("decoding account balance: %w", err)
	}

	return account, nil
}

func normalizeCurrencies(currencies []ledgerdomain.Currency) ([]string, error) {
	if len(currencies) == 0 {
		return nil, fmt.Errorf("%w: at least one currency is required", ErrInvalidArgument)
	}
	if len(currencies) > maxCurrenciesPerWallet {
		return nil, fmt.Errorf(
			"%w: currency count exceeds %d",
			ErrInvalidArgument,
			maxCurrenciesPerWallet,
		)
	}

	codes := make([]string, 0, len(currencies))
	seen := make(map[string]struct{}, len(currencies))
	for _, currency := range currencies {
		code := currency.String()
		if code == "" {
			return nil, fmt.Errorf("%w: invalid currency", ErrInvalidArgument)
		}
		if _, exists := seen[code]; exists {
			return nil, fmt.Errorf("%w: duplicate currency %q", ErrInvalidArgument, code)
		}

		seen[code] = struct{}{}
		codes = append(codes, code)
	}

	slices.Sort(codes)

	return codes, nil
}

func parseUUID(value string) (pgtype.UUID, error) {
	var parsed pgtype.UUID
	if err := parsed.Scan(value); err != nil {
		return pgtype.UUID{}, fmt.Errorf("parsing UUID: %w", err)
	}
	if !parsed.Valid {
		return pgtype.UUID{}, errors.New("parsing UUID: value is empty")
	}

	return parsed, nil
}

func finishTransaction(
	ctx context.Context,
	tx pgx.Tx,
	result *wallet.Wallet,
	resultErr *error,
) {
	rollbackErr := tx.Rollback(ctx)
	if rollbackErr == nil || errors.Is(rollbackErr, pgx.ErrTxClosed) {
		return
	}

	*result = wallet.Wallet{}
	*resultErr = errors.Join(*resultErr, fmt.Errorf("rolling back wallet transaction: %w", rollbackErr))
}
