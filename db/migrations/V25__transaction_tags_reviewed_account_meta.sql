-- V25: Add tags, is_reviewed to transactions; institution, account_number to accounts.

-- Transactions: tags (array of text labels) + is_reviewed flag
ALTER TABLE transactions
  ADD COLUMN IF NOT EXISTS tags TEXT[] NOT NULL DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS is_reviewed BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_tx_unreviewed ON transactions(account_id)
  WHERE deleted_at IS NULL AND is_reviewed = FALSE;

-- Accounts: institution name + masked account number
ALTER TABLE accounts
  ADD COLUMN IF NOT EXISTS institution TEXT NULL,
  ADD COLUMN IF NOT EXISTS account_number TEXT NULL;
