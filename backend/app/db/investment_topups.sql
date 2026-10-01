ALTER TABLE accounts ADD COLUMN IF NOT EXISTS investment_tracking_mode VARCHAR(10)
    CHECK (investment_tracking_mode IS NULL OR investment_tracking_mode = 'amount');
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS investment_cost_basis BIGINT
    CHECK (investment_cost_basis IS NULL OR investment_cost_basis >= 0);
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS investment_value_estimated BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS investment_topups (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    source_account_id UUID NOT NULL REFERENCES accounts(id),
    target_account_id UUID NOT NULL REFERENCES accounts(id),
    amount BIGINT NOT NULL CHECK (amount > 0),
    date TIMESTAMPTZ NOT NULL,
    notes VARCHAR(500) NOT NULL DEFAULT '',
    idempotency_key VARCHAR(128) NOT NULL,
    request_fingerprint VARCHAR(64) NOT NULL,
    recurring_execution_id UUID REFERENCES recurring_executions(id) ON DELETE SET NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, idempotency_key),
    CHECK (source_account_id <> target_account_id)
);

CREATE INDEX IF NOT EXISTS idx_investment_topups_target ON investment_topups(user_id, target_account_id);
CREATE INDEX IF NOT EXISTS idx_investment_topups_source ON investment_topups(user_id, source_account_id);

ALTER TABLE recurring_rules DROP CONSTRAINT IF EXISTS chk_recurring_transfer_target;
ALTER TABLE recurring_rules ADD CONSTRAINT chk_recurring_transfer_target
    CHECK (type NOT IN ('transfer', 'investment_topup') OR target_account_id IS NOT NULL);
ALTER TABLE recurring_rules DROP CONSTRAINT IF EXISTS chk_recurring_investment_topup;
ALTER TABLE recurring_rules ADD CONSTRAINT chk_recurring_investment_topup
    CHECK (type <> 'investment_topup' OR (
        auto_post = FALSE AND is_payroll_allocation = FALSE
        AND category_id IS NULL AND obligation_id IS NULL
    ));
