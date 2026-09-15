-- V2__personal_finance_os.sql: Personal Finance OS additions
-- Adds goals (savings targets) and obligations (debts/commitments), linking to ledger transactions

-- 6. Goals (Savings targets with pacing and deadlines)
CREATE TABLE IF NOT EXISTS goals (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    target_amount BIGINT NOT NULL,
    current_amount BIGINT NOT NULL DEFAULT 0,
    target_date DATE NULL,
    color VARCHAR(30) NOT NULL DEFAULT '#10b981',
    icon VARCHAR(50) NOT NULL DEFAULT 'target',
    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_goal_user_name UNIQUE (user_id, name),
    CONSTRAINT chk_goal_target_positive CHECK (target_amount > 0),
    CONSTRAINT chk_goal_current_non_negative CHECK (current_amount >= 0)
);
CREATE INDEX IF NOT EXISTS idx_goals_user_id ON goals(user_id);

-- 7. Obligations (Debts, loans, and recurring commitments)
CREATE TABLE IF NOT EXISTS obligations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    total_amount BIGINT NOT NULL,
    remaining_amount BIGINT NOT NULL,
    due_date DATE NULL,
    minimum_payment BIGINT NULL,
    notes TEXT NULL,
    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_obligation_user_name UNIQUE (user_id, name),
    CONSTRAINT chk_obligation_total_positive CHECK (total_amount > 0),
    CONSTRAINT chk_obligation_remaining_non_negative CHECK (remaining_amount >= 0)
);
CREATE INDEX IF NOT EXISTS idx_obligations_user_id ON obligations(user_id);

-- Extend Transactions with optional goal and obligation links
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS goal_id UUID NULL REFERENCES goals(id) ON DELETE SET NULL;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS obligation_id UUID NULL REFERENCES obligations(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_transactions_goal ON transactions(goal_id);
CREATE INDEX IF NOT EXISTS idx_transactions_obligation ON transactions(obligation_id);
