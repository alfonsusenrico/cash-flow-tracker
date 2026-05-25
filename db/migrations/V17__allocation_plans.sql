-- Phase 2: Allocation Plans + Items
-- One plan per monthly period. Items define how income is distributed across buckets.
-- mode: fixed (absolute amount) | percent (% of expected_income)
-- status: pending | partial | funded | overflowed

CREATE TABLE IF NOT EXISTS allocation_plans (
  plan_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id          UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  period_id        UUID NULL REFERENCES monthly_periods(period_id) ON DELETE SET NULL,
  month            TEXT NOT NULL,           -- 'YYYY-MM' denormalized for easy lookup
  expected_income  BIGINT NOT NULL DEFAULT 0 CHECK (expected_income >= 0),
  status           TEXT NOT NULL DEFAULT 'draft'
                     CHECK (status IN ('draft','active','closed')),
  notes            TEXT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (user_id, month)
);

CREATE TABLE IF NOT EXISTS allocation_items (
  item_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  plan_id          UUID NOT NULL REFERENCES allocation_plans(plan_id) ON DELETE CASCADE,
  bucket_id        UUID NULL REFERENCES buckets(bucket_id) ON DELETE SET NULL,
  label            TEXT NOT NULL,           -- display name (copied from bucket or custom)
  mode             TEXT NOT NULL DEFAULT 'percent'
                     CHECK (mode IN ('fixed','percent')),
  value            NUMERIC(18,4) NOT NULL CHECK (value >= 0),  -- amount or percentage
  priority         INT NOT NULL DEFAULT 50,
  planned_amount   BIGINT NOT NULL DEFAULT 0,   -- computed: value resolved to IDR
  funded_amount    BIGINT NOT NULL DEFAULT 0,   -- how much has actually been moved
  status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending','partial','funded','overflowed')),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_allocation_plans_user_month ON allocation_plans(user_id, month);
CREATE INDEX IF NOT EXISTS idx_allocation_items_plan ON allocation_items(plan_id);
CREATE INDEX IF NOT EXISTS idx_allocation_items_bucket ON allocation_items(bucket_id);
