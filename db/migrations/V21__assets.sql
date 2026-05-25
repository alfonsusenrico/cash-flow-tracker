-- Phase 4: Assets
-- class: stock | etf | mutual_fund | bond | crypto | metal | property | other

CREATE TABLE IF NOT EXISTS assets (
  asset_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id     UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  name        TEXT NOT NULL,
  class       TEXT NOT NULL DEFAULT 'other'
                CHECK (class IN ('stock','etf','mutual_fund','bond','crypto','metal','property','other')),
  currency    TEXT NOT NULL DEFAULT 'IDR',
  ticker      TEXT NULL,
  is_active   BOOLEAN NOT NULL DEFAULT TRUE,
  notes       TEXT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (user_id, name)
);

CREATE INDEX IF NOT EXISTS idx_assets_user_id ON assets(user_id);
