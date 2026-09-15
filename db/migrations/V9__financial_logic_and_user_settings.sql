-- Migration V9: Financial logic alignment and user settings
ALTER TABLE users ADD COLUMN IF NOT EXISTS name VARCHAR(150) NULL;
ALTER TABLE users ADD COLUMN IF NOT EXISTS emergency_fund_multiplier INT NOT NULL DEFAULT 6;
ALTER TABLE users ADD COLUMN IF NOT EXISTS monthly_spending_budget BIGINT NULL;

-- Purge erroneous investment valuation transactions that were misclassified as salary income
DELETE FROM transactions WHERE notes LIKE 'Penyesuaian Nilai Investasi%';
