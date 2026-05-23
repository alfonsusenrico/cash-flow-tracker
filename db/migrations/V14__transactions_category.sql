-- Phase 1: Link transactions to categories.
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS category_id UUID NULL
  REFERENCES categories(category_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_tx_category_id ON transactions(category_id) WHERE deleted_at IS NULL;
