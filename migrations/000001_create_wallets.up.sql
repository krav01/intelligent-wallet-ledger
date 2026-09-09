CREATE TABLE wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id UUID NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT wallets_status_check CHECK (status IN ('active', 'frozen', 'closed')),
    CONSTRAINT wallets_timestamps_check CHECK (updated_at >= created_at)
);

CREATE INDEX wallets_owner_id_idx ON wallets (owner_id);

CREATE TABLE accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_id UUID NOT NULL REFERENCES wallets (id) ON DELETE CASCADE,
    currency TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT accounts_currency_check CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT accounts_status_check CHECK (status IN ('active', 'frozen', 'closed')),
    CONSTRAINT accounts_timestamps_check CHECK (updated_at >= created_at),
    CONSTRAINT accounts_wallet_currency_key UNIQUE (wallet_id, currency),
    CONSTRAINT accounts_id_currency_key UNIQUE (id, currency)
);

CREATE TABLE account_balances (
    account_id UUID PRIMARY KEY,
    currency TEXT NOT NULL,
    balance_minor BIGINT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT account_balances_currency_check CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT account_balances_nonnegative_check CHECK (balance_minor >= 0),
    CONSTRAINT account_balances_version_check CHECK (version >= 0),
    CONSTRAINT account_balances_account_currency_fkey
        FOREIGN KEY (account_id, currency)
        REFERENCES accounts (id, currency)
        ON DELETE CASCADE
);

COMMENT ON TABLE account_balances IS
    'Transactional read model; immutable ledger postings remain the financial source of truth.';
