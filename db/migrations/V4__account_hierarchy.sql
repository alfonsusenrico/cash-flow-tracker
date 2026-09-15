-- Migration V4: Add parent_id to accounts for hierarchical sub-accounts / pockets
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS parent_id UUID NULL REFERENCES accounts(id) ON DELETE CASCADE;
CREATE INDEX IF NOT EXISTS idx_accounts_parent ON accounts(parent_id);
