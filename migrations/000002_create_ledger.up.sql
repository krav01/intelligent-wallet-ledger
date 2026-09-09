ALTER TABLE accounts
    ADD COLUMN account_type TEXT NOT NULL DEFAULT 'customer',
    ADD CONSTRAINT accounts_type_check
        CHECK (account_type IN ('customer', 'system')),
    ADD CONSTRAINT accounts_id_currency_type_key
        UNIQUE (id, currency, account_type);

ALTER TABLE account_balances
    DROP CONSTRAINT account_balances_account_currency_fkey,
    DROP CONSTRAINT account_balances_nonnegative_check,
    ADD COLUMN account_type TEXT NOT NULL DEFAULT 'customer',
    ADD CONSTRAINT account_balances_type_check
        CHECK (account_type IN ('customer', 'system')),
    ADD CONSTRAINT account_balances_nonnegative_customer_check
        CHECK (account_type = 'system' OR balance_minor >= 0),
    ADD CONSTRAINT account_balances_account_currency_type_fkey
        FOREIGN KEY (account_id, currency, account_type)
        REFERENCES accounts (id, currency, account_type)
        ON DELETE CASCADE;

CREATE TABLE journal_entries (
    id UUID PRIMARY KEY,
    entry_type TEXT NOT NULL,
    reverses_entry_id UUID,
    recorded_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT journal_entries_type_check
        CHECK (entry_type IN ('standard', 'reversal')),
    CONSTRAINT journal_entries_reversal_shape_check CHECK (
        (entry_type = 'standard' AND reverses_entry_id IS NULL)
        OR (
            entry_type = 'reversal'
            AND reverses_entry_id IS NOT NULL
            AND reverses_entry_id <> id
        )
    ),
    CONSTRAINT journal_entries_reverses_entry_fkey
        FOREIGN KEY (reverses_entry_id)
        REFERENCES journal_entries (id)
        ON DELETE RESTRICT
);

CREATE UNIQUE INDEX journal_entries_one_reversal_idx
    ON journal_entries (reverses_entry_id)
    WHERE reverses_entry_id IS NOT NULL;

CREATE TABLE postings (
    journal_entry_id UUID NOT NULL,
    position SMALLINT NOT NULL,
    account_id UUID NOT NULL,
    currency TEXT NOT NULL,
    amount_minor BIGINT NOT NULL,
    CONSTRAINT postings_pkey PRIMARY KEY (journal_entry_id, position),
    CONSTRAINT postings_entry_fkey
        FOREIGN KEY (journal_entry_id)
        REFERENCES journal_entries (id)
        ON DELETE RESTRICT,
    CONSTRAINT postings_account_currency_fkey
        FOREIGN KEY (account_id, currency)
        REFERENCES accounts (id, currency)
        ON DELETE RESTRICT,
    CONSTRAINT postings_position_check CHECK (position BETWEEN 0 AND 1023),
    CONSTRAINT postings_currency_check CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT postings_amount_nonzero_check CHECK (amount_minor <> 0),
    CONSTRAINT postings_amount_reversible_check
        CHECK (amount_minor <> '-9223372036854775808'::BIGINT)
);

CREATE INDEX postings_account_entry_idx
    ON postings (account_id, journal_entry_id);

COMMENT ON TABLE journal_entries IS
    'Immutable financial source of truth; corrections append reversal entries.';

COMMENT ON COLUMN accounts.account_type IS
    'Customer accounts cannot be negative; system accounts provide the settlement side.';
