CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS users (
  username TEXT PRIMARY KEY,
  password_hash TEXT NOT NULL,
  full_name TEXT NOT NULL,
  default_payday_day INT NOT NULL DEFAULT 25 CHECK (default_payday_day BETWEEN 1 AND 31),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS accounts (
  account_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
  account_name TEXT NOT NULL,
  parent_account_id UUID NULL REFERENCES accounts(account_id) ON DELETE SET NULL,
  opening_balance BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (username, account_name)
);

CREATE TABLE IF NOT EXISTS transactions (
  transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  account_id UUID NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
  transaction_type TEXT NOT NULL CHECK (transaction_type IN ('debit','credit')),
  is_cycle_topup BOOLEAN NOT NULL DEFAULT FALSE,
  transaction_name TEXT NOT NULL,
  amount BIGINT NOT NULL CHECK (amount > 0),
  date TIMESTAMPTZ NOT NULL,
  is_transfer BOOLEAN NOT NULL DEFAULT FALSE,
  transfer_id UUID NULL,
  deleted_at TIMESTAMPTZ NULL,
  deleted_by TEXT NULL REFERENCES users(username) ON DELETE SET NULL,
  delete_reason TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ck_tx_cycle_topup_debit CHECK (NOT is_cycle_topup OR transaction_type = 'debit')
);

CREATE TABLE IF NOT EXISTS budgets (
  budget_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
  account_id UUID NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
  month TEXT NOT NULL,
  amount BIGINT NOT NULL CHECK (amount >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (username, account_id, month)
);

CREATE TABLE IF NOT EXISTS payday_overrides (
  username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
  month TEXT NOT NULL,
  payday_day INT NOT NULL CHECK (payday_day BETWEEN 1 AND 31),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (username, month)
);

CREATE TABLE IF NOT EXISTS transaction_audit (
  audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  transaction_id UUID NOT NULL,
  account_id UUID NULL,
  username TEXT NOT NULL,
  action TEXT NOT NULL CHECK (action IN ('soft_delete')),
  payload JSONB NOT NULL,
  performed_by TEXT NULL REFERENCES users(username) ON DELETE SET NULL,
  performed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_accounts_username ON accounts(username);
CREATE INDEX IF NOT EXISTS idx_tx_account_date ON transactions(account_id, date, transaction_id);
CREATE INDEX IF NOT EXISTS idx_tx_date ON transactions(date);
CREATE INDEX IF NOT EXISTS idx_tx_date_type ON transactions(date, transaction_type);
CREATE INDEX IF NOT EXISTS idx_tx_transfer_id ON transactions(transfer_id);
CREATE INDEX IF NOT EXISTS idx_tx_cycle_topup_active ON transactions(account_id, date) WHERE deleted_at IS NULL AND is_cycle_topup = TRUE;
CREATE INDEX IF NOT EXISTS idx_tx_active_account_date ON transactions(account_id, date, transaction_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_tx_deleted_at ON transactions(deleted_at);
CREATE INDEX IF NOT EXISTS idx_budgets_username_month ON budgets(username, month);
CREATE INDEX IF NOT EXISTS idx_payday_overrides_username_month ON payday_overrides(username, month);
CREATE INDEX IF NOT EXISTS idx_tx_audit_username_time ON transaction_audit(username, performed_at DESC);
CREATE INDEX IF NOT EXISTS idx_tx_audit_transaction_time ON transaction_audit(transaction_id, performed_at DESC);
