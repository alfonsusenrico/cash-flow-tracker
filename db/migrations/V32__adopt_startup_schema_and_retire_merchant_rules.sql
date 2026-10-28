-- V32__adopt_startup_schema_and_retire_merchant_rules.sql
-- Flyway becomes the only schema owner. The API no longer runs startup DDL, so this migration
-- adopts the schema and data repair that existed only in that startup block, and retires the
-- merchant_category_rules table that merchant_memory replaced.

-- 1. Schema that only the startup block created. Production already has both (no-op there);
--    a fresh database gets them here.
ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS default_funding_account_id UUID NULL REFERENCES accounts(id) ON DELETE SET NULL;

-- Backstop for idempotent ledger writes: one transaction per owner and idempotency key.
CREATE UNIQUE INDEX IF NOT EXISTS uq_tx_user_idempotency
    ON transactions(user_id, idempotency_key) WHERE idempotency_key IS NOT NULL;

-- 2. Paid-off debts are archived (the startup block repeated this on every boot).
UPDATE obligations
SET is_archived = TRUE, updated_at = NOW()
WHERE remaining_amount <= 0 AND is_archived = FALSE;

-- 3. merchant_category_rules was never written by the application. Drop it only when it is
--    empty so that no owner data can be lost; otherwise stop the deployment for a decision.
DO $$
DECLARE
    rule_count BIGINT;
BEGIN
    IF to_regclass('merchant_category_rules') IS NOT NULL THEN
        SELECT COUNT(*) INTO rule_count FROM merchant_category_rules;
        IF rule_count > 0 THEN
            RAISE EXCEPTION
                'V32: merchant_category_rules holds % row(s); migrate them to merchant_memory or drop them explicitly before deploying',
                rule_count;
        END IF;
        DROP TABLE merchant_category_rules;
    END IF;
END $$;
