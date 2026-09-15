-- Migration V8: Add color and display_order to accounts
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS color VARCHAR(30) NOT NULL DEFAULT '#3b82f6';
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS display_order INT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_accounts_display_order ON accounts(user_id, display_order);
