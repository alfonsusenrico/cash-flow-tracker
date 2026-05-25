-- Phase 2: Strategy Rules
-- Rules that define how income should be distributed when it arrives.
-- trigger: income_arrival | manual
-- mode: fixed | percent | target_balance | overflow
-- overflow_to_bucket_id: where surplus goes after this rule is satisfied

CREATE TABLE IF NOT EXISTS strategy_rules (
  rule_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id              UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  name                 TEXT NOT NULL,
  trigger              TEXT NOT NULL DEFAULT 'manual'
                         CHECK (trigger IN ('income_arrival','manual')),
  mode                 TEXT NOT NULL DEFAULT 'percent'
                         CHECK (mode IN ('fixed','percent','target_balance','overflow')),
  target_bucket_id     UUID NULL REFERENCES buckets(bucket_id) ON DELETE SET NULL,
  value                NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (value >= 0),
  cap                  BIGINT NULL CHECK (cap >= 0),    -- max to allocate per run
  floor                BIGINT NULL CHECK (floor >= 0),  -- min balance before rule fires
  priority             INT NOT NULL DEFAULT 50,
  is_active            BOOLEAN NOT NULL DEFAULT TRUE,
  notes                TEXT NULL,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_strategy_rules_user_id ON strategy_rules(user_id);
CREATE INDEX IF NOT EXISTS idx_strategy_rules_user_active ON strategy_rules(user_id, priority)
  WHERE is_active = TRUE;
