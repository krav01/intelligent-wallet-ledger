CREATE TABLE transfers (
    id UUID PRIMARY KEY,
    requester_id UUID NOT NULL,
    idempotency_key TEXT COLLATE "C" NOT NULL,
    source_account_id UUID NOT NULL,
    destination_account_id UUID NOT NULL,
    currency TEXT NOT NULL,
    amount_minor BIGINT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT transfers_requester_key_key
        UNIQUE (requester_id, idempotency_key),
    CONSTRAINT transfers_distinct_accounts_check
        CHECK (source_account_id <> destination_account_id),
    CONSTRAINT transfers_currency_check
        CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT transfers_amount_positive_check
        CHECK (amount_minor > 0),
    CONSTRAINT transfers_idempotency_key_check
        CHECK (
            octet_length(idempotency_key) BETWEEN 1 AND 128
            AND idempotency_key ~ '^[!-~]+$' COLLATE "C"
        ),
    CONSTRAINT transfers_journal_entry_fkey
        FOREIGN KEY (id)
        REFERENCES journal_entries (id)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT transfers_source_account_currency_fkey
        FOREIGN KEY (source_account_id, currency)
        REFERENCES accounts (id, currency)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT transfers_destination_account_currency_fkey
        FOREIGN KEY (destination_account_id, currency)
        REFERENCES accounts (id, currency)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX transfers_source_account_id_idx
    ON transfers (source_account_id);

CREATE INDEX transfers_destination_account_id_idx
    ON transfers (destination_account_id);

COMMENT ON TABLE transfers IS
    'Committed customer transfers; scoped keys provide atomic idempotent replay.';
