-- Turn scheduled rules into non-posting expectations. Ledger rows, rule links and occurrence history
-- are untouched; the previous automatic-posting choice is kept for audit and deliberate rollback.
-- Every statement is repeatable, and Flyway applies the file in one transaction.
ALTER TABLE recurring_rules ADD COLUMN IF NOT EXISTS legacy_auto_post BOOLEAN;
ALTER TABLE recurring_rules ADD COLUMN IF NOT EXISTS converted_to_expectation_at TIMESTAMPTZ;

UPDATE recurring_rules
SET legacy_auto_post = auto_post, converted_to_expectation_at = NOW()
WHERE converted_to_expectation_at IS NULL;

UPDATE recurring_rules SET auto_post = FALSE WHERE auto_post;

-- Re-enabling automatic posting requires dropping this constraint on purpose.
ALTER TABLE recurring_rules DROP CONSTRAINT IF EXISTS chk_recurring_rules_non_posting;
ALTER TABLE recurring_rules ADD CONSTRAINT chk_recurring_rules_non_posting CHECK (auto_post = FALSE);

-- An occurrence can now be met by linking a recorded transaction, or be skipped by the owner.
ALTER TABLE recurring_executions DROP CONSTRAINT IF EXISTS chk_recurring_execution_status;
ALTER TABLE recurring_executions ADD CONSTRAINT chk_recurring_execution_status
    CHECK (status IN ('processing', 'succeeded', 'failed', 'linked', 'skipped'));

-- One recorded transaction can satisfy at most one expectation occurrence.
CREATE UNIQUE INDEX IF NOT EXISTS uq_recurring_executions_linked_transaction
    ON recurring_executions (transaction_id)
    WHERE status = 'linked' AND transaction_id IS NOT NULL;
