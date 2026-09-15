-- V10__recurring_rules.sql: Automated Recurring Transactions & Payroll Allocations

CREATE TABLE IF NOT EXISTS recurring_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    type VARCHAR(20) NOT NULL, -- 'expense', 'transfer', 'income'
    amount BIGINT NOT NULL CHECK (amount > 0),
    source_account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    target_account_id UUID NULL REFERENCES accounts(id) ON DELETE SET NULL,
    category_id UUID NULL REFERENCES categories(id) ON DELETE SET NULL,
    obligation_id UUID NULL REFERENCES obligations(id) ON DELETE SET NULL,
    schedule_type VARCHAR(30) NOT NULL DEFAULT 'monthly_day', -- 'monthly_day', 'payday', 'weekly'
    schedule_day INT NULL, -- day of month (1-31) or day of week (1=Mon, 7=Sun)
    notes TEXT NULL,
    is_payroll_allocation BOOLEAN NOT NULL DEFAULT FALSE,
    auto_post BOOLEAN NOT NULL DEFAULT FALSE,
    last_executed_at TIMESTAMPTZ NULL,
    next_due_date DATE NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_recurring_transfer_target CHECK (type != 'transfer' OR target_account_id IS NOT NULL),
    CONSTRAINT chk_recurring_distinct_accounts CHECK (target_account_id IS NULL OR source_account_id != target_account_id)
);

CREATE INDEX IF NOT EXISTS idx_recurring_rules_user ON recurring_rules(user_id);
CREATE INDEX IF NOT EXISTS idx_recurring_rules_due ON recurring_rules(user_id, is_active, next_due_date);
CREATE INDEX IF NOT EXISTS idx_recurring_rules_payroll ON recurring_rules(user_id, is_payroll_allocation);

-- Add recurring_rule_id to transactions
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS recurring_rule_id UUID NULL REFERENCES recurring_rules(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_transactions_recurring_rule ON transactions(recurring_rule_id);
