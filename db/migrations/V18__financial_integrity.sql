-- Durable movement linkage, reconciliation state, and recurring occurrence identity.

ALTER TABLE transactions ADD COLUMN IF NOT EXISTS movement_id UUID NULL;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS movement_role VARCHAR(10) NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_transactions_movement_role'
    ) THEN
        ALTER TABLE transactions
            ADD CONSTRAINT chk_transactions_movement_role
            CHECK (
                (movement_id IS NULL AND movement_role IS NULL)
                OR (movement_id IS NOT NULL AND movement_role IN ('outbound', 'inbound'))
            );
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_transactions_movement_role
    ON transactions (movement_id, movement_role)
    WHERE movement_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_transactions_movement_id
    ON transactions (movement_id)
    WHERE movement_id IS NOT NULL;

ALTER TABLE accounts ADD COLUMN IF NOT EXISTS reconciliation_required BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS reconciliation_reason VARCHAR(100) NULL;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS reconciliation_event_id UUID NULL;

CREATE TABLE IF NOT EXISTS recurring_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recurring_rule_id UUID NOT NULL REFERENCES recurring_rules(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    scheduled_for DATE NOT NULL,
    status VARCHAR(20) NOT NULL,
    error_code VARCHAR(100) NULL,
    error_detail TEXT NULL,
    transaction_id UUID NULL REFERENCES transactions(id) ON DELETE SET NULL,
    movement_id UUID NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_recurring_execution_occurrence UNIQUE (recurring_rule_id, scheduled_for),
    CONSTRAINT chk_recurring_execution_status CHECK (status IN ('processing', 'succeeded', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_recurring_executions_user_status
    ON recurring_executions (user_id, status, scheduled_for DESC);
