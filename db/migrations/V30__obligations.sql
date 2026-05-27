-- V30: External payables and receivables.
-- These are user-facing obligations (utang/piutang), distinct from internal_loans
-- which model account-to-account shortfall handling.

CREATE TABLE IF NOT EXISTS counterparties (
  counterparty_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  type TEXT NOT NULL DEFAULT 'person'
    CHECK (type IN ('person', 'client', 'vendor', 'institution', 'other')),
  notes TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (user_id, name)
);

CREATE TABLE IF NOT EXISTS obligations (
  obligation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN ('receivable', 'payable')),
  counterparty_id UUID NULL REFERENCES counterparties(counterparty_id) ON DELETE SET NULL,
  title TEXT NOT NULL,
  description TEXT NULL,
  principal_amount BIGINT NOT NULL CHECK (principal_amount > 0),
  outstanding_amount BIGINT NOT NULL CHECK (outstanding_amount >= 0),
  currency TEXT NOT NULL DEFAULT 'IDR',
  status TEXT NOT NULL DEFAULT 'open'
    CHECK (status IN ('open', 'partial', 'settled', 'cancelled', 'written_off')),
  issue_date DATE NOT NULL DEFAULT CURRENT_DATE,
  due_date DATE NULL,
  default_account_id UUID NULL REFERENCES accounts(account_id) ON DELETE SET NULL,
  category_id UUID NULL REFERENCES categories(category_id) ON DELETE SET NULL,
  notes TEXT NULL,
  recurrence_frequency TEXT NOT NULL DEFAULT 'none'
    CHECK (recurrence_frequency IN ('none', 'weekly', 'monthly', 'quarterly', 'yearly')),
  auto_post_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  auto_post_day INT NULL CHECK (auto_post_day IS NULL OR auto_post_day BETWEEN 1 AND 31),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  settled_at TIMESTAMPTZ NULL
);

CREATE TABLE IF NOT EXISTS obligation_settlements (
  settlement_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  obligation_id UUID NOT NULL REFERENCES obligations(obligation_id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  transaction_id UUID NULL UNIQUE REFERENCES transactions(transaction_id) ON DELETE SET NULL,
  account_id UUID NOT NULL REFERENCES accounts(account_id) ON DELETE RESTRICT,
  amount BIGINT NOT NULL CHECK (amount > 0),
  settled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  notes TEXT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  reversed_at TIMESTAMPTZ NULL,
  reversed_by TEXT NULL REFERENCES users(username) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_counterparties_user_name
  ON counterparties(user_id, lower(name));

CREATE INDEX IF NOT EXISTS idx_obligations_user_kind_status
  ON obligations(user_id, kind, status);

CREATE INDEX IF NOT EXISTS idx_obligations_user_due
  ON obligations(user_id, due_date)
  WHERE status IN ('open', 'partial');

CREATE INDEX IF NOT EXISTS idx_obligation_settlements_obligation
  ON obligation_settlements(obligation_id, created_at DESC)
  WHERE reversed_at IS NULL;
