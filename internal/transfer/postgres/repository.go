// Package postgres persists idempotent customer transfers in PostgreSQL.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	ledgerdomain "github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
	ledgerpostgres "github.com/krav01/intelligent-wallet-ledger/internal/ledger/postgres"
	transferdomain "github.com/krav01/intelligent-wallet-ledger/internal/transfer/domain"
)

const maxTransactionAttempts = 3

var (
	// ErrInvalidArgument indicates malformed repository input.
	ErrInvalidArgument = errors.New("transfer repository: invalid argument")
	// ErrNotFound indicates that the destination account does not exist.
	ErrNotFound = errors.New("transfer repository: not found")
	// ErrUnauthorized indicates that the requester does not own the source account.
	ErrUnauthorized = errors.New("transfer repository: source account is not owned by requester")
	// ErrInactiveAccount indicates that either transfer account is not active.
	ErrInactiveAccount = errors.New("transfer repository: account is not active")
	// ErrNonCustomerAccount indicates that a system account was used for a customer transfer.
	ErrNonCustomerAccount = errors.New("transfer repository: account is not a customer account")
	// ErrCurrencyMismatch indicates that an account uses another currency.
	ErrCurrencyMismatch = errors.New("transfer repository: account currency mismatch")
	// ErrIdempotencyConflict indicates reuse of a scoped key for different intent.
	ErrIdempotencyConflict = errors.New("transfer repository: idempotency conflict")
	// ErrAlreadyExists indicates that the transfer ID is already stored.
	ErrAlreadyExists = errors.New("transfer repository: transfer already exists")
	// ErrInsufficientFunds indicates that the source balance cannot cover the transfer.
	ErrInsufficientFunds = errors.New("transfer repository: insufficient funds")
	// ErrAmountOverflow indicates that a balance cannot represent the transfer effect.
	ErrAmountOverflow = errors.New("transfer repository: amount overflow")
	// ErrCorruptData indicates that stored rows violate transfer domain invariants.
	ErrCorruptData = errors.New("transfer repository: corrupt data")
	// ErrSerializationFailure indicates that bounded transaction retries were exhausted.
	ErrSerializationFailure = errors.New("transfer repository: serialization retries exhausted")
)

const (
	insertTransferQuery = `
INSERT INTO transfers (
    id,
    requester_id,
    idempotency_key,
    source_account_id,
    destination_account_id,
    currency,
    amount_minor,
    requested_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (requester_id, idempotency_key) DO NOTHING
RETURNING id::text`

	selectTransferByKeyQuery = `
SELECT
    id::text,
    requester_id::text,
    idempotency_key,
    source_account_id::text,
    destination_account_id::text,
    currency,
    amount_minor,
    requested_at
FROM transfers
WHERE requester_id = $1 AND idempotency_key = $2`

	selectAccountsQuery = `
SELECT
    account.id::text,
    wallet.owner_id::text,
    account.status,
    account.account_type,
    account.currency
FROM accounts AS account
JOIN wallets AS wallet ON wallet.id = account.wallet_id
WHERE account.id = ANY($1::uuid[])
ORDER BY account.id
FOR SHARE OF account`
)

// Repository stores successful transfers and their ledger effects atomically.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a PostgreSQL transfer repository.
func NewRepository(pool *pgxpool.Pool) (*Repository, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: pool is required", ErrInvalidArgument)
	}

	return &Repository{pool: pool}, nil
}

// Create stores a transfer exactly once within the requester's key namespace.
func (r *Repository) Create(
	ctx context.Context,
	transfer transferdomain.Transfer,
) (transferdomain.Transfer, error) {
	prepared, err := prepareTransfer(transfer)
	if err != nil {
		return transferdomain.Transfer{}, err
	}

	var lastErr error
	for attempt := range maxTransactionAttempts {
		if err := ctx.Err(); err != nil {
			return transferdomain.Transfer{}, fmt.Errorf("creating transfer: %w", err)
		}

		created, createErr := r.createOnce(ctx, prepared)
		if createErr == nil {
			return created, nil
		}
		lastErr = createErr
		if !isRetryableTransaction(createErr) {
			return transferdomain.Transfer{}, classifyCreateError(createErr)
		}
		if attempt == maxTransactionAttempts-1 {
			break
		}
	}

	return transferdomain.Transfer{}, fmt.Errorf("%w: %w", ErrSerializationFailure, lastErr)
}

type preparedTransfer struct {
	transfer      transferdomain.Transfer
	id            pgtype.UUID
	requesterID   pgtype.UUID
	sourceID      pgtype.UUID
	destinationID pgtype.UUID
	entry         ledgerdomain.JournalEntry
}

type accountRecord struct {
	id          string
	ownerID     string
	status      string
	accountType string
	currency    string
}

func prepareTransfer(transfer transferdomain.Transfer) (preparedTransfer, error) {
	validated, err := transferdomain.NewTransfer(transferdomain.NewTransferParams{
		ID:                   transfer.ID(),
		IdempotencyKey:       transfer.IdempotencyKey(),
		RequesterID:          transfer.RequesterID(),
		SourceAccountID:      transfer.SourceAccountID(),
		DestinationAccountID: transfer.DestinationAccountID(),
		Amount:               transfer.Amount(),
		RequestedAt:          transfer.RequestedAt(),
	})
	if err != nil {
		return preparedTransfer{}, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}

	id, err := parseUUID(validated.ID())
	if err != nil {
		return preparedTransfer{}, fmt.Errorf("%w: transfer ID: %w", ErrInvalidArgument, err)
	}
	requesterID, err := parseUUID(validated.RequesterID())
	if err != nil {
		return preparedTransfer{}, fmt.Errorf("%w: requester ID: %w", ErrInvalidArgument, err)
	}
	sourceID, err := parseUUID(validated.SourceAccountID())
	if err != nil {
		return preparedTransfer{}, fmt.Errorf("%w: source account ID: %w", ErrInvalidArgument, err)
	}
	destinationID, err := parseUUID(validated.DestinationAccountID())
	if err != nil {
		return preparedTransfer{}, fmt.Errorf("%w: destination account ID: %w", ErrInvalidArgument, err)
	}
	entry, err := validated.JournalEntry()
	if err != nil {
		return preparedTransfer{}, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}

	return preparedTransfer{
		transfer:      validated,
		id:            id,
		requesterID:   requesterID,
		sourceID:      sourceID,
		destinationID: destinationID,
		entry:         entry,
	}, nil
}

func (r *Repository) createOnce(
	ctx context.Context,
	prepared preparedTransfer,
) (created transferdomain.Transfer, err error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return transferdomain.Transfer{}, fmt.Errorf("beginning transfer transaction: %w", err)
	}
	defer finishTransaction(ctx, tx, &err)

	inserted, err := reserveTransfer(ctx, tx, prepared)
	if err != nil {
		return transferdomain.Transfer{}, err
	}
	if !inserted {
		stored, selectErr := selectTransferByKey(ctx, tx, prepared)
		if selectErr != nil {
			return transferdomain.Transfer{}, selectErr
		}
		if !sameIntent(stored, prepared.transfer) {
			return transferdomain.Transfer{}, ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return transferdomain.Transfer{}, fmt.Errorf("committing transfer replay: %w", err)
		}

		return stored, nil
	}

	if err := authorizeAccounts(ctx, tx, prepared); err != nil {
		return transferdomain.Transfer{}, err
	}
	if err := ledgerpostgres.PostTx(ctx, tx, prepared.entry); err != nil {
		return transferdomain.Transfer{}, mapLedgerError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return transferdomain.Transfer{}, fmt.Errorf("committing transfer transaction: %w", err)
	}

	return prepared.transfer, nil
}

func reserveTransfer(ctx context.Context, tx pgx.Tx, prepared preparedTransfer) (bool, error) {
	var id string
	err := tx.QueryRow(
		ctx,
		insertTransferQuery,
		prepared.id,
		prepared.requesterID,
		prepared.transfer.IdempotencyKey(),
		prepared.sourceID,
		prepared.destinationID,
		prepared.transfer.Amount().Currency().String(),
		prepared.transfer.Amount().MinorUnits(),
		prepared.transfer.RequestedAt(),
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reserving transfer key: %w", err)
	}

	return true, nil
}

func selectTransferByKey(
	ctx context.Context,
	tx pgx.Tx,
	prepared preparedTransfer,
) (transferdomain.Transfer, error) {
	var params transferdomain.NewTransferParams
	var currencyCode string
	var amountMinor int64
	err := tx.QueryRow(
		ctx,
		selectTransferByKeyQuery,
		prepared.requesterID,
		prepared.transfer.IdempotencyKey(),
	).Scan(
		&params.ID,
		&params.RequesterID,
		&params.IdempotencyKey,
		&params.SourceAccountID,
		&params.DestinationAccountID,
		&currencyCode,
		&amountMinor,
		&params.RequestedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return transferdomain.Transfer{}, retryableReplayError{}
	}
	if err != nil {
		return transferdomain.Transfer{}, fmt.Errorf("selecting transfer replay: %w", err)
	}

	currency, err := ledgerdomain.ParseCurrency(currencyCode)
	if err != nil {
		return transferdomain.Transfer{}, fmt.Errorf("%w: transfer currency: %w", ErrCorruptData, err)
	}
	params.Amount, err = ledgerdomain.NewMoney(amountMinor, currency)
	if err != nil {
		return transferdomain.Transfer{}, fmt.Errorf("%w: transfer amount: %w", ErrCorruptData, err)
	}
	stored, err := transferdomain.NewTransfer(params)
	if err != nil {
		return transferdomain.Transfer{}, fmt.Errorf("%w: transfer: %w", ErrCorruptData, err)
	}

	return stored, nil
}

func authorizeAccounts(ctx context.Context, tx pgx.Tx, prepared preparedTransfer) error {
	rows, err := tx.Query(ctx, selectAccountsQuery, []pgtype.UUID{prepared.sourceID, prepared.destinationID})
	if err != nil {
		return fmt.Errorf("selecting transfer accounts: %w", err)
	}
	defer rows.Close()

	records := make(map[string]accountRecord, 2)
	for rows.Next() {
		var record accountRecord
		if err := rows.Scan(
			&record.id,
			&record.ownerID,
			&record.status,
			&record.accountType,
			&record.currency,
		); err != nil {
			return fmt.Errorf("scanning transfer account: %w", err)
		}
		records[record.id] = record
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating transfer accounts: %w", err)
	}

	source, sourceExists := records[prepared.transfer.SourceAccountID()]
	if !sourceExists || source.ownerID != prepared.transfer.RequesterID() {
		return ErrUnauthorized
	}
	destination, destinationExists := records[prepared.transfer.DestinationAccountID()]
	if !destinationExists {
		return ErrNotFound
	}
	for _, record := range []accountRecord{source, destination} {
		if record.status != "active" {
			return fmt.Errorf("%w: account %s", ErrInactiveAccount, record.id)
		}
		if record.accountType != "customer" {
			return fmt.Errorf("%w: account %s", ErrNonCustomerAccount, record.id)
		}
		if record.currency != prepared.transfer.Amount().Currency().String() {
			return fmt.Errorf("%w: account %s", ErrCurrencyMismatch, record.id)
		}
	}

	return nil
}

func sameIntent(left, right transferdomain.Transfer) bool {
	return left.RequesterID() == right.RequesterID() &&
		left.SourceAccountID() == right.SourceAccountID() &&
		left.DestinationAccountID() == right.DestinationAccountID() &&
		left.Amount() == right.Amount()
}

func mapLedgerError(err error) error {
	switch {
	case errors.Is(err, ledgerpostgres.ErrInsufficientFunds):
		return fmt.Errorf("%w: %w", ErrInsufficientFunds, err)
	case errors.Is(err, ledgerpostgres.ErrAmountOverflow):
		return fmt.Errorf("%w: %w", ErrAmountOverflow, err)
	case errors.Is(err, ledgerpostgres.ErrAlreadyExists):
		return fmt.Errorf("%w: %w", ErrAlreadyExists, err)
	case errors.Is(err, ledgerpostgres.ErrCurrencyMismatch):
		return fmt.Errorf("%w: %w", ErrCurrencyMismatch, err)
	case errors.Is(err, ledgerpostgres.ErrNotFound):
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	default:
		return err
	}
}

type retryableReplayError struct{}

func (retryableReplayError) Error() string {
	return "transfer replay is not visible in transaction snapshot"
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

func classifyCreateError(err error) error {
	postgresError, ok := errors.AsType[*pgconn.PgError](err)
	if !ok {
		return err
	}
	if postgresError.Code == "23505" && postgresError.ConstraintName == "transfers_pkey" {
		return fmt.Errorf("%w: %w", ErrAlreadyExists, err)
	}

	return err
}

func isRetryableTransaction(err error) bool {
	var replayError retryableReplayError
	if errors.As(err, &replayError) {
		return true
	}

	postgresError, ok := errors.AsType[*pgconn.PgError](err)
	return ok && (postgresError.Code == "40001" || postgresError.Code == "40P01")
}

func finishTransaction(ctx context.Context, tx pgx.Tx, resultErr *error) {
	rollbackErr := tx.Rollback(ctx)
	if rollbackErr == nil || errors.Is(rollbackErr, pgx.ErrTxClosed) {
		return
	}

	*resultErr = errors.Join(*resultErr, fmt.Errorf("rolling back transfer transaction: %w", rollbackErr))
}
