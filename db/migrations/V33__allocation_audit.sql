-- MVP-4: Audit trail for allocation plan and item mutations.
-- Rows are retained even after plan deletion (ON DELETE SET NULL).

CREATE TABLE IF NOT EXISTS allocation_plan_audit (
  audit_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  plan_id      UUID NULL REFERENCES allocation_plans(plan_id) ON DELETE SET NULL,
  user_id      UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  performed_by TEXT NOT NULL,
  action       TEXT NOT NULL CHECK (action IN
                 ('created','updated','activated','closed','deleted','funded','reopened')),
  before_state JSONB NULL,
  after_state  JSONB NULL,
  reason       TEXT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_plan_audit_plan
  ON allocation_plan_audit(plan_id, created_at DESC);

CREATE TABLE IF NOT EXISTS allocation_item_audit (
  audit_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  item_id      UUID NOT NULL,
  plan_id      UUID NULL REFERENCES allocation_plans(plan_id) ON DELETE SET NULL,
  user_id      UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  performed_by TEXT NOT NULL,
  action       TEXT NOT NULL CHECK (action IN
                 ('created','updated','funded','deleted')),
  before_state JSONB NULL,
  after_state  JSONB NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_item_audit_plan
  ON allocation_item_audit(plan_id, created_at DESC);
