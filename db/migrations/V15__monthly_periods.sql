-- Phase 1: Monthly periods as first-class entities.
-- Replaces the ad-hoc payday-range derivation with a stored, closeable record.
-- status: 'open' = current cycle; 'closed' = user confirmed; 'reviewed' = notes written.

CREATE TABLE IF NOT EXISTS monthly_periods (
  period_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id      UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  month        TEXT NOT NULL,          -- 'YYYY-MM'
  from_date    DATE NOT NULL,
  to_date      DATE NOT NULL,
  payday_day   INT  NOT NULL CHECK (payday_day BETWEEN 1 AND 31),
  status       TEXT NOT NULL DEFAULT 'open'
                 CHECK (status IN ('open', 'closed', 'reviewed')),
  notes        TEXT NULL,
  closed_at    TIMESTAMPTZ NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (user_id, month)
);

CREATE INDEX IF NOT EXISTS idx_periods_user_id_status ON monthly_periods(user_id, status);
CREATE INDEX IF NOT EXISTS idx_periods_user_id_month  ON monthly_periods(user_id, month);

-- Backfill: create a closed period row for every (user, YYYY-MM) that has transactions,
-- using the user's default_payday_day for the range.
-- from_date = payday of previous month, to_date = day before payday of this month.
-- Current month gets status='open'.
DO $$
DECLARE
  current_ym TEXT := to_char(now(), 'YYYY-MM');
BEGIN
  INSERT INTO monthly_periods (user_id, month, from_date, to_date, payday_day, status)
  SELECT DISTINCT
    u.user_id,
    to_char(t.date AT TIME ZONE 'UTC', 'YYYY-MM') AS month,
    -- from_date: payday of previous month
    (date_trunc('month', t.date AT TIME ZONE 'UTC') - interval '1 month'
      + (u.default_payday_day - 1) * interval '1 day')::date AS from_date,
    -- to_date: day before payday of this month (capped at today for open month)
    LEAST(
      (date_trunc('month', t.date AT TIME ZONE 'UTC')
        + (u.default_payday_day - 1) * interval '1 day' - interval '1 day')::date,
      CURRENT_DATE
    ) AS to_date,
    u.default_payday_day,
    CASE
      WHEN to_char(t.date AT TIME ZONE 'UTC', 'YYYY-MM') = current_ym THEN 'open'
      ELSE 'closed'
    END AS status
  FROM transactions t
  JOIN accounts a ON a.account_id = t.account_id
  JOIN users u ON u.user_id = a.user_id
  WHERE t.deleted_at IS NULL
  ON CONFLICT (user_id, month) DO NOTHING;
END $$;
