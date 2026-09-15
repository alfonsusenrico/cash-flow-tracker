-- V3__goal_accounts.sql: Link savings goals to physical accounts/pockets
CREATE TABLE IF NOT EXISTS goal_accounts (
    goal_id UUID NOT NULL REFERENCES goals(id) ON DELETE CASCADE,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (goal_id, account_id)
);

CREATE INDEX IF NOT EXISTS idx_goal_accounts_goal ON goal_accounts(goal_id);
CREATE INDEX IF NOT EXISTS idx_goal_accounts_account ON goal_accounts(account_id);
