ALTER TABLE users
  ADD COLUMN IF NOT EXISTS default_payday_day INT NOT NULL DEFAULT 25
  CHECK (default_payday_day BETWEEN 1 AND 31);

ALTER TABLE transactions
  ADD COLUMN IF NOT EXISTS is_transfer BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE transactions
  ADD COLUMN IF NOT EXISTS transfer_id UUID NULL;

CREATE TABLE IF NOT EXISTS payday_overrides (
  username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
  month TEXT NOT NULL,
  payday_day INT NOT NULL CHECK (payday_day BETWEEN 1 AND 31),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (username, month)
);

CREATE INDEX IF NOT EXISTS idx_tx_transfer_id ON transactions(transfer_id);
CREATE INDEX IF NOT EXISTS idx_payday_overrides_username_month ON payday_overrides(username, month);
