-- Allocation funding automation and generated monthly spending limits.

ALTER TABLE allocation_plans
  ADD COLUMN IF NOT EXISTS funding_source_account_id UUID NULL REFERENCES accounts(account_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS auto_fund_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  ADD COLUMN IF NOT EXISTS activated_at TIMESTAMPTZ NULL;

ALTER TABLE allocation_items
  ADD COLUMN IF NOT EXISTS target_account_id UUID NULL REFERENCES accounts(account_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS include_in_emergency_base BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE budgets
  ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'manual'
    CHECK (source IN ('manual','allocation')),
  ADD COLUMN IF NOT EXISTS allocation_plan_id UUID NULL REFERENCES allocation_plans(plan_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS allocation_run_id UUID NULL;

CREATE TABLE IF NOT EXISTS allocation_funding_runs (
  run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  plan_id UUID NOT NULL REFERENCES allocation_plans(plan_id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  source_account_id UUID NULL REFERENCES accounts(account_id) ON DELETE SET NULL,
  trigger_type TEXT NOT NULL DEFAULT 'manual'
    CHECK (trigger_type IN ('manual','automatic')),
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending','succeeded','failed','skipped')),
  amount BIGINT NOT NULL DEFAULT 0 CHECK (amount >= 0),
  failure_reason TEXT NULL,
  transfer_ids UUID[] NOT NULL DEFAULT ARRAY[]::uuid[],
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_allocation_items_target_account
  ON allocation_items(target_account_id);

CREATE INDEX IF NOT EXISTS idx_allocation_runs_plan
  ON allocation_funding_runs(plan_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_allocation_runs_user_status
  ON allocation_funding_runs(user_id, status, created_at DESC);
