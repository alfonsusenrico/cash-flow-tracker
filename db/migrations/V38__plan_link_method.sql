-- Record how a plan occurrence was linked to a recorded transaction, whether that link applied
-- debt progress, and which pairs the owner rejected so they are never linked automatically again.
-- Every statement is repeatable; Flyway applies the file in one transaction.
ALTER TABLE recurring_executions ADD COLUMN IF NOT EXISTS link_method VARCHAR(10);
ALTER TABLE recurring_executions ADD COLUMN IF NOT EXISTS obligation_applied BOOLEAN NOT NULL DEFAULT FALSE;

-- Links made before this migration were all chosen by the owner.
UPDATE recurring_executions SET link_method = 'owner' WHERE status = 'linked' AND link_method IS NULL;

ALTER TABLE recurring_executions DROP CONSTRAINT IF EXISTS chk_recurring_execution_link_method;
ALTER TABLE recurring_executions ADD CONSTRAINT chk_recurring_execution_link_method
    CHECK (link_method IS NULL OR link_method IN ('auto', 'owner'));

CREATE TABLE IF NOT EXISTS recurring_link_rejections (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    recurring_rule_id UUID NOT NULL REFERENCES recurring_rules(id) ON DELETE CASCADE,
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    scheduled_for DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (recurring_rule_id, transaction_id)
);

-- Identifiers and actions only; no amounts, names or notes.
CREATE TABLE IF NOT EXISTS recurring_link_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    recurring_rule_id UUID NOT NULL REFERENCES recurring_rules(id) ON DELETE CASCADE,
    scheduled_for DATE NOT NULL,
    transaction_id UUID NULL REFERENCES transactions(id) ON DELETE SET NULL,
    action VARCHAR(10) NOT NULL CHECK (action IN ('link', 'unlink')),
    method VARCHAR(10) NOT NULL CHECK (method IN ('auto', 'owner')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_recurring_link_events_rule
    ON recurring_link_events (recurring_rule_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_recurring_rules_due_active
    ON recurring_rules (user_id, next_due_date) WHERE is_active;
