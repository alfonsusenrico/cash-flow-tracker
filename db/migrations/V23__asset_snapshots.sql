-- Phase 4: Asset Snapshots (manual price entries)
-- One row per (asset, date) — latest snapshot = current price.

CREATE TABLE IF NOT EXISTS asset_snapshots (
  snapshot_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  asset_id     UUID NOT NULL REFERENCES assets(asset_id) ON DELETE CASCADE,
  as_of_date   DATE NOT NULL,
  unit_price   BIGINT NOT NULL CHECK (unit_price >= 0),  -- price per unit in IDR
  currency     TEXT NOT NULL DEFAULT 'IDR',
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (asset_id, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_asset_date ON asset_snapshots(asset_id, as_of_date DESC);
