-- Phase 0: Introduce user_id UUID as the canonical tenant key.
-- Strategy: add user_id, populate from username, add FK constraints,
-- then keep username as a unique display name (not the PK).
-- Existing code continues to work via username; new code uses user_id.

-- 1. Add user_id to users table
ALTER TABLE users ADD COLUMN IF NOT EXISTS user_id UUID NOT NULL DEFAULT gen_random_uuid();
CREATE UNIQUE INDEX IF NOT EXISTS uq_users_user_id ON users(user_id);

-- 2. Add user_id columns to all owner-scoped tables (nullable first for backfill)
ALTER TABLE accounts         ADD COLUMN IF NOT EXISTS user_id UUID;
ALTER TABLE budgets          ADD COLUMN IF NOT EXISTS user_id UUID;
ALTER TABLE payday_overrides ADD COLUMN IF NOT EXISTS user_id UUID;
ALTER TABLE transaction_audit ADD COLUMN IF NOT EXISTS user_id UUID;
ALTER TABLE transaction_receipts ADD COLUMN IF NOT EXISTS user_id UUID;
ALTER TABLE internal_loans   ADD COLUMN IF NOT EXISTS user_id UUID;
ALTER TABLE api_keys         ADD COLUMN IF NOT EXISTS user_id UUID;

-- 3. Backfill user_id from username join
UPDATE accounts         a SET user_id = u.user_id FROM users u WHERE u.username = a.username;
UPDATE budgets          b SET user_id = u.user_id FROM users u WHERE u.username = b.username;
UPDATE payday_overrides p SET user_id = u.user_id FROM users u WHERE u.username = p.username;
UPDATE transaction_audit ta SET user_id = u.user_id FROM users u WHERE u.username = ta.username;
UPDATE transaction_receipts tr SET user_id = u.user_id FROM users u WHERE u.username = tr.username;
UPDATE internal_loans   il SET user_id = u.user_id FROM users u WHERE u.username = il.username;
UPDATE api_keys         ak SET user_id = u.user_id FROM users u WHERE u.username = ak.username;

-- 4. Set NOT NULL now that backfill is done
ALTER TABLE accounts         ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE budgets          ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE payday_overrides ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE transaction_audit ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE transaction_receipts ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE internal_loans   ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE api_keys         ALTER COLUMN user_id SET NOT NULL;

-- 5. Add FK constraints referencing users(user_id)
ALTER TABLE accounts         ADD CONSTRAINT fk_accounts_user_id         FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE;
ALTER TABLE budgets          ADD CONSTRAINT fk_budgets_user_id          FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE;
ALTER TABLE payday_overrides ADD CONSTRAINT fk_payday_overrides_user_id FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE;
ALTER TABLE transaction_audit ADD CONSTRAINT fk_tx_audit_user_id        FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE;
ALTER TABLE transaction_receipts ADD CONSTRAINT fk_receipts_user_id     FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE;
ALTER TABLE internal_loans   ADD CONSTRAINT fk_loans_user_id            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE;
ALTER TABLE api_keys         ADD CONSTRAINT fk_api_keys_user_id         FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE;

-- 6. Add indexes on user_id for the hot query paths
CREATE INDEX IF NOT EXISTS idx_accounts_user_id         ON accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_budgets_user_id_month    ON budgets(user_id, month);
CREATE INDEX IF NOT EXISTS idx_api_keys_user_id_active  ON api_keys(user_id) WHERE revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_loans_user_id_status     ON internal_loans(user_id, status);

-- NOTE: username columns are kept for backward compatibility.
-- The application dual-reads (username for session auth, user_id for data queries).
-- username columns will be dropped in a future migration once all code paths migrate.
