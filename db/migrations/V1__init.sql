CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE users (
  username TEXT PRIMARY KEY,
  password_hash TEXT NOT NULL,
  full_name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE accounts (
  account_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
  account_name TEXT NOT NULL,
  parent_account_id UUID NULL REFERENCES accounts(account_id) ON DELETE SET NULL,
  opening_balance BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (username, account_name)
);

CREATE TABLE transactions (
  transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  account_id UUID NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
  transaction_type TEXT NOT NULL CHECK (transaction_type IN ('debit','credit')),
  transaction_name TEXT NOT NULL,
  amount BIGINT NOT NULL CHECK (amount > 0),
  date TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE budgets (
  budget_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
  account_id UUID NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
  month TEXT NOT NULL,
  amount BIGINT NOT NULL CHECK (amount >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (username, account_id, month)
);

CREATE INDEX idx_accounts_username ON accounts(username);
CREATE INDEX idx_tx_account_date ON transactions(account_id, date, transaction_id);
CREATE INDEX idx_tx_date ON transactions(date);
CREATE INDEX idx_tx_date_type ON transactions(date, transaction_type);
CREATE INDEX idx_budgets_username_month ON budgets(username, month);
