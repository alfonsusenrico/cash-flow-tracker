-- Account roles and saved layouts (organize-accounts-by-use D1, D5). Additive and safe to run twice.
-- `role` ("Kegunaan") says what an account's money is for. It is added nullable, only NULL rows are
-- backfilled from today's meaning, and only then does it get its default and NOT NULL, so a rerun never
-- overwrites a role the owner chose. The backfill keeps today's figures: credit is utang, an investment
-- product is investasi, a goal-linked account (today's savings destination) is tabungan, the rest harian.
-- An older release ignores the column and the table. Rollback: leave the column, the trigger and the
-- table in place, unused by the older code.
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS role TEXT NULL;

UPDATE accounts a
SET role = CASE
    WHEN a.type = 'credit' THEN 'utang'
    WHEN a.type = 'investment' OR a.instrument_type IS NOT NULL THEN 'investasi'
    WHEN EXISTS (SELECT 1 FROM goal_accounts ga WHERE ga.account_id = a.id) THEN 'tabungan'
    ELSE 'harian'
END
WHERE a.role IS NULL;

ALTER TABLE accounts ALTER COLUMN role SET DEFAULT 'harian';
ALTER TABLE accounts ALTER COLUMN role SET NOT NULL;

ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_role_check;
ALTER TABLE accounts ADD CONSTRAINT accounts_role_check
    CHECK (role IN ('harian', 'tabungan', 'dana_investasi', 'investasi', 'utang'));

-- The type decides the role where it can (D1): credit is utang and an investment product investasi, and a
-- cash, bank or wallet account never keeps either. The API refuses a contradicting role before it gets
-- here; this trigger covers writers that do not set a role, such as a broker position created from a
-- notification or an older release after a rollback, so a stale role never survives a type change.
CREATE OR REPLACE FUNCTION accounts_role_follows_type() RETURNS trigger AS $$
BEGIN
    IF NEW.type = 'credit' THEN
        NEW.role := 'utang';
    ELSIF NEW.type = 'investment' OR NEW.instrument_type IS NOT NULL THEN
        NEW.role := 'investasi';
    ELSIF NEW.role IS NULL OR NEW.role IN ('utang', 'investasi') THEN
        NEW.role := 'harian';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS accounts_role_follows_type ON accounts;
CREATE TRIGGER accounts_role_follows_type
    BEFORE INSERT OR UPDATE OF type, instrument_type, role ON accounts
    FOR EACH ROW EXECUTE FUNCTION accounts_role_follows_type();

-- The owner's order of sections and cards on Rekening & Kantong, one row per grouping, so the order is
-- the same on every device. `sections` is an ordered JSON array of section keys (role keys for 'use',
-- top-level account ids for 'rekening'); `cards` maps each section key to an ordered array of account ids.
-- The API normalizes both against the owner's current accounts on every read and write.
CREATE TABLE IF NOT EXISTS account_layouts (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    grouping TEXT NOT NULL CHECK (grouping IN ('use', 'rekening')),
    sections JSONB NOT NULL,
    cards JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, grouping)
);
