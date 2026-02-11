ALTER TABLE transactions
  ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ NULL;

ALTER TABLE transactions
  ADD COLUMN IF NOT EXISTS deleted_by TEXT NULL REFERENCES users(username) ON DELETE SET NULL;

ALTER TABLE transactions
  ADD COLUMN IF NOT EXISTS delete_reason TEXT NULL;

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

CREATE INDEX IF NOT EXISTS idx_tx_active_account_date
  ON transactions(account_id, date, transaction_id)
  WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_tx_deleted_at ON transactions(deleted_at);
CREATE INDEX IF NOT EXISTS idx_tx_audit_username_time ON transaction_audit(username, performed_at DESC);
CREATE INDEX IF NOT EXISTS idx_tx_audit_transaction_time ON transaction_audit(transaction_id, performed_at DESC);
