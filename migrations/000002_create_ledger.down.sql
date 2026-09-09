DROP TABLE postings;
DROP TABLE journal_entries;

ALTER TABLE account_balances
    DROP CONSTRAINT account_balances_account_currency_type_fkey,
    DROP CONSTRAINT account_balances_type_check,
    DROP CONSTRAINT account_balances_nonnegative_customer_check,
    ADD CONSTRAINT account_balances_nonnegative_check CHECK (balance_minor >= 0),
    ADD CONSTRAINT account_balances_account_currency_fkey
        FOREIGN KEY (account_id, currency)
        REFERENCES accounts (id, currency)
        ON DELETE CASCADE,
    DROP COLUMN account_type;

ALTER TABLE accounts
    DROP CONSTRAINT accounts_id_currency_type_key,
    DROP CONSTRAINT accounts_type_check,
    DROP COLUMN account_type;
