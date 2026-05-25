-- Phase 4: Net Worth Snapshots
-- Daily computed roll-up: liquid (cash accounts) + invested (assets) - liabilities.

CREATE TABLE IF NOT EXISTS net_worth_snapshots (
  snapshot_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id          UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  as_of_date       DATE NOT NULL,
  liquid_assets    BIGINT NOT NULL DEFAULT 0,    -- sum of account balances
  invested_assets  BIGINT NOT NULL DEFAULT 0,    -- sum of holding qty * latest price
  liabilities      BIGINT NOT NULL DEFAULT 0,    -- reserved for future liability tracking
  net_worth        BIGINT NOT NULL DEFAULT 0,    -- liquid + invested - liabilities
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (user_id, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_net_worth_user_date ON net_worth_snapshots(user_id, as_of_date DESC);
