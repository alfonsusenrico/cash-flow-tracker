-- Phase 3: Goal Contributions
-- Records each time money is moved toward a goal.
-- source: manual | allocation (from an allocation plan item)

CREATE TABLE IF NOT EXISTS goal_contributions (
  contribution_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  goal_id          UUID NOT NULL REFERENCES financial_goals(goal_id) ON DELETE CASCADE,
  amount           BIGINT NOT NULL CHECK (amount > 0),
  date             TIMESTAMPTZ NOT NULL DEFAULT now(),
  source           TEXT NOT NULL DEFAULT 'manual'
                     CHECK (source IN ('manual','allocation')),
  notes            TEXT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_goal_contributions_goal ON goal_contributions(goal_id);
CREATE INDEX IF NOT EXISTS idx_goal_contributions_date ON goal_contributions(goal_id, date DESC);
