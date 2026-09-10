// Package postgres persists immutable ledger entries and transactional balance snapshots.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/krav01/intelligent-wallet-ledger/internal/ledger/domain"
)

const maxTransactionAttempts = 3

var (
	// ErrInvalidArgument indicates malformed repository input.
	ErrInvalidArgument = errors.New("ledger repository: invalid argument")
	// ErrNotFound indicates that an entry, account, or reversal source does not exist.
	ErrNotFound = errors.New("ledger repository: not found")
	// ErrAlreadyExists indicates that a journal entry ID is already stored.
	ErrAlreadyExists = errors.New("ledger repository: journal entry already exists")
	// ErrAlreadyReversed indicates that an entry already has a reversal.
	ErrAlreadyReversed = errors.New("ledger repository: journal entry already reversed")
	// ErrCurrencyMismatch indicates that a posting currency differs from its account currency.
	ErrCurrencyMismatch = errors.New("ledger repository: account currency mismatch")
	// ErrInsufficientFunds indicates that a customer balance would become negative.
	ErrInsufficientFunds = errors.New("ledger repository: insufficient funds")
	// ErrAmountOverflow indicates that a balance or version would exceed its storage range.
	ErrAmountOverflow = errors.New("ledger repository: amount overflow")
	// ErrInvalidReversal indicates that reversal postings do not invert the persisted source.
	ErrInvalidReversal = errors.New("ledger repository: invalid reversal")
	// ErrCorruptData indicates that stored rows violate ledger domain invariants.
	ErrCorruptData = errors.New("ledger repository: corrupt data")
	// ErrSerializationFailure indicates that bounded transaction retries were exhausted.
	ErrSerializationFailure = errors.New("ledger repository: serialization retries exhausted")
)

const (
	lockBalancesQuery = `
SELECT account_id::text, currency, account_type, balance_minor, version
FROM account_balances
WHERE account_id = ANY($1::uuid[])
ORDER BY account_id
FOR UPDATE`

	insertJournalEntryQuery = `
INSERT INTO journal_entries (id, entry_type, reverses_entry_id, recorded_at)
VALUES ($1, $2, $3, $4)`

	insertPostingQuery = `
INSERT INTO postings (journal_entry_id, position, account_id, currency, amount_minor)
VALUES ($1, $2, $3, $4, $5)`

	updateBalanceQuery = `
UPDATE account_balances
SET balance_minor = $2,
    version = $3,
    updated_at = CURRENT_TIMESTAMP
WHERE account_id = $1`

	selectEntryQuery = `
SELECT
    id::text,
    entry_type,
    COALESCE(reverses_entry_id::text, ''),
    recorded_at
FROM journal_entries
WHERE id = $1`

	selectEntryForKeyShareQuery = `
SELECT
    id::text,
    entry_type,
    COALESCE(reverses_entry_id::text, ''),
    recorded_at
FROM journal_entries
WHERE id = $1
FOR KEY SHARE`

	selectPostingsQuery = `
SELECT account_id::text, currency, amount_minor
FROM postings
WHERE journal_entry_id = $1
ORDER BY position`

	selectReversalExistsQuery = `
SELECT EXISTS (
    SELECT 1
    FROM journal_entries
    WHERE reverses_entry_id = $1
)`
)

// Repository stores ledger entries and updates account snapshots in one transaction.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a PostgreSQL ledger repository.
func NewRepository(pool *pgxpool.Pool) (*Repository, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: pool is required", ErrInvalidArgument)
	}

	return &Repository{pool: pool}, nil
}

// Post atomically stores an entry, its postings, and all affected balance snapshots.
func (r *Repository) Post(ctx context.Context, entry domain.JournalEntry) error {
	prepared, err := prepareEntry(entry)
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := range maxTransactionAttempts {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("posting journal entry: %w", err)
		}

		lastErr = r.postOnce(ctx, prepared)
		if lastErr == nil {
			return nil
		}
		if !isRetryableTransaction(lastErr) {
			return classifyPostError(lastErr)
		}
		if attempt == maxTransactionAttempts-1 {
			break
		}
	}

	return fmt.Errorf("%w: %w", ErrSerializationFailure, lastErr)
}

// PostTx stores an entry and applies its balance effects inside the supplied transaction.
// The caller owns transaction commit, rollback, isolation, and retries.
func PostTx(ctx context.Context, tx pgx.Tx, entry domain.JournalEntry) error {
	if tx == nil {
		return fmt.Errorf("%w: transaction is required", ErrInvalidArgument)
	}

	prepared, err := prepareEntry(entry)
	if err != nil {
		return err
	}

	return classifyPostError(postPrepared(ctx, tx, prepared))
}

// Get returns one transactionally consistent journal entry.
func (r *Repository) Get(ctx context.Context, entryID string) (result domain.JournalEntry, err error) {
	parsedID, err := parseUUID(entryID)
	if err != nil {
		return domain.JournalEntry{}, fmt.Errorf("%w: entry ID: %w", ErrInvalidArgument, err)
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return domain.JournalEntry{}, fmt.Errorf("beginning ledger read transaction: %w", err)
	}
	defer finishTransaction(ctx, tx, &err)

	record, err := selectEntry(ctx, tx, parsedID, false)
	if err != nil {
		return domain.JournalEntry{}, err
	}
	postings, err := selectPostings(ctx, tx, parsedID)
	if err != nil {
		return domain.JournalEntry{}, err
	}

	result, err = restoreEntry(ctx, tx, record, postings)
	if err != nil {
		return domain.JournalEntry{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.JournalEntry{}, fmt.Errorf("committing ledger read transaction: %w", err)
	}

	return result, nil
}

type preparedEntry struct {
	entry      domain.JournalEntry
	id         pgtype.UUID
	reversesID *pgtype.UUID
	postings   []preparedPosting
	changes    []accountChange
}

type preparedPosting struct {
	accountID pgtype.UUID
	currency  domain.Currency
	amount    int64
}

type accountChange struct {
	accountID  pgtype.UUID
	currency   domain.Currency
	delta      int64
	newBalance int64
	newVersion int64
}

type accountAccumulator struct {
	accountID pgtype.UUID
	currency  domain.Currency
	total     *big.Int
}

type storedEntry struct {
	id         string
	entryType  string
	reversesID string
	recordedAt time.Time
}

func prepareEntry(entry domain.JournalEntry) (preparedEntry, error) {
	id, err := parseUUID(entry.ID())
	if err != nil {
		return preparedEntry{}, fmt.Errorf("%w: entry ID: %w", ErrInvalidArgument, err)
	}

	postings := entry.Postings()
	if _, err := domain.NewJournalEntry(domain.NewJournalEntryParams{
		ID:         entry.ID(),
		Postings:   postings,
		RecordedAt: entry.RecordedAt(),
	}); err != nil {
		return preparedEntry{}, fmt.Errorf("%w: journal entry: %w", ErrInvalidArgument, err)
	}

	var reversesID *pgtype.UUID
	switch entry.Type() {
	case domain.JournalEntryTypeStandard:
		if entry.ReversesEntryID() != "" {
			return preparedEntry{}, fmt.Errorf("%w: standard entry has reversal source", ErrInvalidArgument)
		}
	case domain.JournalEntryTypeReversal:
		parsedSourceID, parseErr := parseUUID(entry.ReversesEntryID())
		if parseErr != nil {
			return preparedEntry{}, fmt.Errorf("%w: reversal source ID: %w", ErrInvalidArgument, parseErr)
		}
		if parsedSourceID == id {
			return preparedEntry{}, fmt.Errorf("%w: reversal source equals entry ID", ErrInvalidArgument)
		}
		reversesID = &parsedSourceID
	default:
		return preparedEntry{}, fmt.Errorf("%w: journal entry type is invalid", ErrInvalidArgument)
	}

	preparedPostings, changes, err := preparePostings(postings)
	if err != nil {
		return preparedEntry{}, err
	}

	return preparedEntry{
		entry:      entry,
		id:         id,
		reversesID: reversesID,
		postings:   preparedPostings,
		changes:    changes,
	}, nil
}

func preparePostings(postings []domain.Posting) ([]preparedPosting, []accountChange, error) {
	prepared := make([]preparedPosting, 0, len(postings))
	accumulators := make(map[string]*accountAccumulator)
	for _, posting := range postings {
		accountID, err := parseUUID(posting.AccountID())
		if err != nil {
			return nil, nil, fmt.Errorf("%w: account ID: %w", ErrInvalidArgument, err)
		}

		amount := posting.Amount()
		prepared = append(prepared, preparedPosting{
			accountID: accountID,
			currency:  amount.Currency(),
			amount:    amount.MinorUnits(),
		})

		key := accountID.String()
		accumulator, exists := accumulators[key]
		if !exists {
			accumulator = &accountAccumulator{
				accountID: accountID,
				currency:  amount.Currency(),
				total:     new(big.Int),
			}
			accumulators[key] = accumulator
		}
		if accumulator.currency != amount.Currency() {
			return nil, nil, fmt.Errorf("%w: account %s", ErrCurrencyMismatch, key)
		}
		accumulator.total.Add(accumulator.total, big.NewInt(amount.MinorUnits()))
	}

	keys := make([]string, 0, len(accumulators))
	for key := range accumulators {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	changes := make([]accountChange, 0, len(keys))
	for _, key := range keys {
		accumulator := accumulators[key]
		if !accumulator.total.IsInt64() {
			return nil, nil, fmt.Errorf("%w: account %s aggregate", ErrAmountOverflow, key)
		}
		changes = append(changes, accountChange{
			accountID: accumulator.accountID,
			currency:  accumulator.currency,
			delta:     accumulator.total.Int64(),
		})
	}

	return prepared, changes, nil
}

func (r *Repository) postOnce(ctx context.Context, prepared preparedEntry) (err error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("beginning ledger transaction: %w", err)
	}
	defer finishTransaction(ctx, tx, &err)

	if err := postPrepared(ctx, tx, prepared); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing ledger transaction: %w", err)
	}

	return nil
}

func postPrepared(ctx context.Context, tx pgx.Tx, prepared preparedEntry) error {
	if prepared.reversesID != nil {
		if err := validatePersistedReversal(ctx, tx, prepared); err != nil {
			return err
		}
	}
	if err := lockAndCalculateBalances(ctx, tx, prepared.changes); err != nil {
		return err
	}
	if err := insertEntry(ctx, tx, prepared); err != nil {
		return err
	}
	if err := insertPostings(ctx, tx, prepared); err != nil {
		return err
	}
	if err := updateBalances(ctx, tx, prepared.changes); err != nil {
		return err
	}

	return nil
}

func lockAndCalculateBalances(ctx context.Context, tx pgx.Tx, changes []accountChange) error {
	accountIDs := make([]pgtype.UUID, 0, len(changes))
	changesByID := make(map[string]*accountChange, len(changes))
	for index := range changes {
		accountIDs = append(accountIDs, changes[index].accountID)
		changesByID[changes[index].accountID.String()] = &changes[index]
	}

	rows, err := tx.Query(ctx, lockBalancesQuery, accountIDs)
	if err != nil {
		return fmt.Errorf("locking account balances: %w", err)
	}
	defer rows.Close()

	locked := 0
	for rows.Next() {
		var accountID string
		var currencyCode string
		var accountType string
		var balance int64
		var version int64

		if err := rows.Scan(&accountID, &currencyCode, &accountType, &balance, &version); err != nil {
			return fmt.Errorf("scanning locked account balance: %w", err)
		}

		change, exists := changesByID[accountID]
		if !exists {
			return fmt.Errorf("%w: unexpected account %s", ErrCorruptData, accountID)
		}
		if currencyCode != change.currency.String() {
			return fmt.Errorf("%w: account %s", ErrCurrencyMismatch, accountID)
		}

		newBalance, err := checkedAdd(balance, change.delta)
		if err != nil {
			return fmt.Errorf("%w: account %s balance", ErrAmountOverflow, accountID)
		}
		if accountType == "customer" && newBalance < 0 {
			return fmt.Errorf("%w: account %s", ErrInsufficientFunds, accountID)
		}
		if accountType != "customer" && accountType != "system" {
			return fmt.Errorf("%w: account %s type", ErrCorruptData, accountID)
		}
		if version == math.MaxInt64 {
			return fmt.Errorf("%w: account %s version", ErrAmountOverflow, accountID)
		}

		change.newBalance = newBalance
		change.newVersion = version + 1
		locked++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating locked account balances: %w", err)
	}
	if locked != len(changes) {
		return fmt.Errorf("%w: one or more accounts", ErrNotFound)
	}

	return nil
}

func validatePersistedReversal(ctx context.Context, tx pgx.Tx, prepared preparedEntry) error {
	_, err := selectEntry(ctx, tx, *prepared.reversesID, true)
	if err != nil {
		return fmt.Errorf("validating reversal source: %w", err)
	}
	sourcePostings, err := selectPostings(ctx, tx, *prepared.reversesID)
	if err != nil {
		return fmt.Errorf("validating reversal source: %w", err)
	}
	var reversalExists bool
	if err := tx.QueryRow(ctx, selectReversalExistsQuery, *prepared.reversesID).Scan(&reversalExists); err != nil {
		return fmt.Errorf("checking existing reversal: %w", err)
	}
	if reversalExists {
		return ErrAlreadyReversed
	}
	if len(sourcePostings) != len(prepared.postings) {
		return fmt.Errorf("%w: posting count differs", ErrInvalidReversal)
	}

	for index, sourcePosting := range sourcePostings {
		reversalPosting := prepared.postings[index]
		sameAccount := sourcePosting.AccountID() == reversalPosting.accountID.String()
		sameCurrency := sourcePosting.Amount().Currency() == reversalPosting.currency
		oppositeAmount := sourcePosting.Amount().MinorUnits() == -reversalPosting.amount
		if !sameAccount || !sameCurrency || !oppositeAmount {
			return fmt.Errorf("%w: posting %d differs", ErrInvalidReversal, index)
		}
	}

	return nil
}

func insertEntry(ctx context.Context, tx pgx.Tx, prepared preparedEntry) error {
	entryType := "standard"
	var reversesID any
	if prepared.entry.Type() == domain.JournalEntryTypeReversal {
		entryType = "reversal"
		reversesID = *prepared.reversesID
	}

	if _, err := tx.Exec(
		ctx,
		insertJournalEntryQuery,
		prepared.id,
		entryType,
		reversesID,
		prepared.entry.RecordedAt(),
	); err != nil {
		return fmt.Errorf("inserting journal entry: %w", err)
	}

	return nil
}

func insertPostings(ctx context.Context, tx pgx.Tx, prepared preparedEntry) error {
	batch := &pgx.Batch{}
	for index, posting := range prepared.postings {
		batch.Queue(
			insertPostingQuery,
			prepared.id,
			index,
			posting.accountID,
			posting.currency.String(),
			posting.amount,
		)
	}

	results := tx.SendBatch(ctx, batch)
	for range prepared.postings {
		if _, err := results.Exec(); err != nil {
			return errors.Join(
				fmt.Errorf("inserting journal postings: %w", err),
				closeBatch(results),
			)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("closing journal posting batch: %w", err)
	}

	return nil
}

func closeBatch(results pgx.BatchResults) error {
	if err := results.Close(); err != nil {
		return fmt.Errorf("closing failed journal posting batch: %w", err)
	}

	return nil
}

func updateBalances(ctx context.Context, tx pgx.Tx, changes []accountChange) error {
	for _, change := range changes {
		commandTag, err := tx.Exec(
			ctx,
			updateBalanceQuery,
			change.accountID,
			change.newBalance,
			change.newVersion,
		)
		if err != nil {
			return fmt.Errorf("updating account balance: %w", err)
		}
		if commandTag.RowsAffected() != 1 {
			return fmt.Errorf("%w: account %s disappeared", ErrCorruptData, change.accountID.String())
		}
	}

	return nil
}

func selectEntry(
	ctx context.Context,
	tx pgx.Tx,
	entryID pgtype.UUID,
	forKeyShare bool,
) (storedEntry, error) {
	query := selectEntryQuery
	if forKeyShare {
		query = selectEntryForKeyShareQuery
	}

	var record storedEntry
	err := tx.QueryRow(ctx, query, entryID).Scan(
		&record.id,
		&record.entryType,
		&record.reversesID,
		&record.recordedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return storedEntry{}, ErrNotFound
	}
	if err != nil {
		return storedEntry{}, fmt.Errorf("selecting journal entry: %w", err)
	}

	return record, nil
}

func selectPostings(ctx context.Context, tx pgx.Tx, entryID pgtype.UUID) ([]domain.Posting, error) {
	rows, err := tx.Query(ctx, selectPostingsQuery, entryID)
	if err != nil {
		return nil, fmt.Errorf("selecting journal postings: %w", err)
	}
	defer rows.Close()

	postings := []domain.Posting{}
	for rows.Next() {
		var accountID string
		var currencyCode string
		var amountMinor int64
		if err := rows.Scan(&accountID, &currencyCode, &amountMinor); err != nil {
			return nil, fmt.Errorf("scanning journal posting: %w", err)
		}

		currency, err := domain.ParseCurrency(currencyCode)
		if err != nil {
			return nil, fmt.Errorf("%w: posting currency: %w", ErrCorruptData, err)
		}
		amount, err := domain.NewMoney(amountMinor, currency)
		if err != nil {
			return nil, fmt.Errorf("%w: posting amount: %w", ErrCorruptData, err)
		}
		posting, err := domain.NewPosting(accountID, amount)
		if err != nil {
			return nil, fmt.Errorf("%w: posting: %w", ErrCorruptData, err)
		}
		postings = append(postings, posting)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating journal postings: %w", err)
	}

	return postings, nil
}

func restoreEntry(
	ctx context.Context,
	tx pgx.Tx,
	record storedEntry,
	postings []domain.Posting,
) (domain.JournalEntry, error) {
	if record.entryType == "standard" {
		entry, err := domain.NewJournalEntry(domain.NewJournalEntryParams{
			ID:         record.id,
			Postings:   postings,
			RecordedAt: record.recordedAt,
		})
		if err != nil {
			return domain.JournalEntry{}, fmt.Errorf("%w: journal entry: %w", ErrCorruptData, err)
		}

		return entry, nil
	}
	if record.entryType != "reversal" {
		return domain.JournalEntry{}, fmt.Errorf("%w: journal entry type %q", ErrCorruptData, record.entryType)
	}

	sourceID, err := parseUUID(record.reversesID)
	if err != nil {
		return domain.JournalEntry{}, fmt.Errorf("%w: reversal source ID: %w", ErrCorruptData, err)
	}
	sourceRecord, err := selectEntry(ctx, tx, sourceID, false)
	if err != nil {
		return domain.JournalEntry{}, fmt.Errorf("%w: reversal source: %w", ErrCorruptData, err)
	}
	sourcePostings, err := selectPostings(ctx, tx, sourceID)
	if err != nil {
		return domain.JournalEntry{}, fmt.Errorf("%w: reversal source: %w", ErrCorruptData, err)
	}
	source, err := domain.NewJournalEntry(domain.NewJournalEntryParams{
		ID:         sourceRecord.id,
		Postings:   sourcePostings,
		RecordedAt: sourceRecord.recordedAt,
	})
	if err != nil {
		return domain.JournalEntry{}, fmt.Errorf("%w: reversal source: %w", ErrCorruptData, err)
	}
	reversal, err := source.Reverse(record.id, record.recordedAt)
	if err != nil {
		return domain.JournalEntry{}, fmt.Errorf("%w: reversal: %w", ErrCorruptData, err)
	}
	if !equalPostings(reversal.Postings(), postings) {
		return domain.JournalEntry{}, fmt.Errorf("%w: reversal postings differ from source", ErrCorruptData)
	}

	return reversal, nil
}

func equalPostings(left, right []domain.Posting) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].AccountID() != right[index].AccountID() ||
			left[index].Amount() != right[index].Amount() {
			return false
		}
	}

	return true
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

func checkedAdd(left, right int64) (int64, error) {
	if right > 0 && left > math.MaxInt64-right ||
		right < 0 && left < math.MinInt64-right {
		return 0, ErrAmountOverflow
	}

	return left + right, nil
}

func classifyPostError(err error) error {
	postgresError, ok := errors.AsType[*pgconn.PgError](err)
	if !ok {
		return err
	}

	switch {
	case postgresError.Code == "23505" && postgresError.ConstraintName == "journal_entries_pkey":
		return fmt.Errorf("%w: %w", ErrAlreadyExists, err)
	case postgresError.Code == "23505" && postgresError.ConstraintName == "journal_entries_one_reversal_idx":
		return fmt.Errorf("%w: %w", ErrAlreadyReversed, err)
	case postgresError.Code == "23514" &&
		postgresError.ConstraintName == "account_balances_nonnegative_customer_check":
		return fmt.Errorf("%w: %w", ErrInsufficientFunds, err)
	case postgresError.Code == "22003":
		return fmt.Errorf("%w: %w", ErrAmountOverflow, err)
	default:
		return err
	}
}

func isRetryableTransaction(err error) bool {
	postgresError, ok := errors.AsType[*pgconn.PgError](err)
	return ok && (postgresError.Code == "40001" || postgresError.Code == "40P01")
}

func finishTransaction(ctx context.Context, tx pgx.Tx, resultErr *error) {
	rollbackErr := tx.Rollback(ctx)
	if rollbackErr == nil || errors.Is(rollbackErr, pgx.ErrTxClosed) {
		return
	}

	*resultErr = errors.Join(*resultErr, fmt.Errorf("rolling back ledger transaction: %w", rollbackErr))
}
