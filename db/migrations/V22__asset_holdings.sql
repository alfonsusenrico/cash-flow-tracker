-- Phase 4: Asset Holdings
-- One row per position (asset + account/broker combination).

CREATE TABLE IF NOT EXISTS asset_holdings (
  holding_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id      UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  asset_id     UUID NOT NULL REFERENCES assets(asset_id) ON DELETE CASCADE,
  account_id   UUID NULL REFERENCES accounts(account_id) ON DELETE SET NULL,
  quantity     NUMERIC(24,8) NOT NULL DEFAULT 0 CHECK (quantity >= 0),
  cost_basis   BIGINT NOT NULL DEFAULT 0 CHECK (cost_basis >= 0),  -- total cost in IDR
  acquired_at  DATE NOT NULL DEFAULT CURRENT_DATE,
  notes        TEXT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_holdings_user_id ON asset_holdings(user_id);
CREATE INDEX IF NOT EXISTS idx_holdings_asset_id ON asset_holdings(asset_id);
