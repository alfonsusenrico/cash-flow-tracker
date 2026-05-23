-- Phase 0: Add direction column as a generated alias for transaction_type.
-- transaction_type='debit' means money IN (inflow); 'credit' means money OUT (outflow).
-- The generated column makes the semantic explicit without breaking existing queries.
-- New code reads `direction`; old code continues to read `transaction_type`.

ALTER TABLE transactions ADD COLUMN IF NOT EXISTS direction TEXT
  GENERATED ALWAYS AS (
    CASE WHEN transaction_type = 'debit' THEN 'in' ELSE 'out' END
  ) STORED;

CREATE INDEX IF NOT EXISTS idx_tx_direction ON transactions(direction) WHERE deleted_at IS NULL;

-- Also add notes and currency fields used by Phase 1 transaction extensions.
-- Added here so the column exists before any data is written.
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS notes TEXT NULL;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS currency TEXT NOT NULL DEFAULT 'IDR';
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS original_amount BIGINT NULL;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS fx_rate NUMERIC(18,6) NULL;

-- Backfill: existing rows are IDR with no FX conversion needed.
UPDATE transactions SET currency = 'IDR' WHERE currency IS NULL OR currency = '';
