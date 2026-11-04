-- Matched balance points for "Cocokkan saldo" (manual-balance-check D6).
-- Additive and idempotent: one table and one index. An older release ignores the table.
-- Rollback: revert the release and leave the table in place, unused.
-- Order: V47 belongs to a parallel change and must be applied first; Flyway does not run
-- out-of-order migrations, so this file must reach production after V47.

-- A point where the owner confirmed that the bank app showed exactly the recorded balance. The next
-- check of the account starts here. Only exact matches are stored (OD143).
CREATE TABLE IF NOT EXISTS account_balance_matches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    balance BIGINT NOT NULL CHECK (balance >= 0),
    matched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS account_balance_matches_latest
    ON account_balance_matches (user_id, account_id, matched_at DESC);
