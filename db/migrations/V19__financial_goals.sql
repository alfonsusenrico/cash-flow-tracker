-- Phase 3: Financial Goals
-- status: active | paused | completed | cancelled

CREATE TABLE IF NOT EXISTS financial_goals (
  goal_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id            UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  name               TEXT NOT NULL,
  target_amount      BIGINT NOT NULL CHECK (target_amount > 0),
  target_date        DATE NULL,
  current_amount     BIGINT NOT NULL DEFAULT 0 CHECK (current_amount >= 0),
  inflation_rate     NUMERIC(6,4) NOT NULL DEFAULT 0.05,   -- annual, e.g. 0.05 = 5%
  expected_return    NUMERIC(6,4) NOT NULL DEFAULT 0.06,   -- annual, e.g. 0.06 = 6%
  linked_bucket_id   UUID NULL REFERENCES buckets(bucket_id) ON DELETE SET NULL,
  priority           INT NOT NULL DEFAULT 50,
  status             TEXT NOT NULL DEFAULT 'active'
                       CHECK (status IN ('active','paused','completed','cancelled')),
  notes              TEXT NULL,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_goals_user_id ON financial_goals(user_id);
CREATE INDEX IF NOT EXISTS idx_goals_user_status ON financial_goals(user_id, status);
