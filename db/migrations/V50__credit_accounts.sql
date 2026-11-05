-- Credit accounts (add-credit-accounts D2). Additive and safe to run twice: two nullable columns with checks.
-- A credit account (type 'credit') owes money while its balance is negative; only it carries a limit or a
-- payment day, which the API enforces. An older release ignores both columns. Rollback: leave them, unused.
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS credit_limit BIGINT NULL;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS payment_day SMALLINT NULL;

ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_credit_limit_check;
ALTER TABLE accounts ADD CONSTRAINT accounts_credit_limit_check CHECK (credit_limit IS NULL OR credit_limit > 0);
ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_payment_day_check;
ALTER TABLE accounts ADD CONSTRAINT accounts_payment_day_check CHECK (payment_day IS NULL OR payment_day BETWEEN 1 AND 31);
