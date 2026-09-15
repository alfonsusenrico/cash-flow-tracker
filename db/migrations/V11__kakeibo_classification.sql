-- V11__kakeibo_classification.sql
-- Add kakeibo_type classification ('need', 'want', 'saving') to categories and transactions

-- 1. Extend categories with kakeibo_type
ALTER TABLE categories ADD COLUMN IF NOT EXISTS kakeibo_type VARCHAR(20) DEFAULT 'need'
  CHECK (kakeibo_type IN ('need', 'want', 'saving'));

-- Auto-map existing categories based on is_primary flag (from V5)
UPDATE categories
SET kakeibo_type = CASE
  WHEN is_primary = FALSE THEN 'want'
  ELSE 'need'
END
WHERE kakeibo_type IS NULL OR kakeibo_type = 'need';

CREATE INDEX IF NOT EXISTS idx_categories_kakeibo ON categories(user_id, kakeibo_type);

-- 2. Extend transactions with optional transaction-level kakeibo_type override
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS kakeibo_type VARCHAR(20)
  CHECK (kakeibo_type IN ('need', 'want', 'saving'));

CREATE INDEX IF NOT EXISTS idx_transactions_kakeibo ON transactions(user_id, kakeibo_type);
