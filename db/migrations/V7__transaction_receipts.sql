CREATE TABLE IF NOT EXISTS transaction_receipts (
  receipt_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  transaction_id UUID NOT NULL REFERENCES transactions(transaction_id) ON DELETE CASCADE,
  username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
  category TEXT NOT NULL,
  original_filename TEXT NULL,
  original_mime TEXT NOT NULL,
  stored_mime TEXT NOT NULL,
  storage_encoding TEXT NOT NULL DEFAULT 'identity'
    CHECK (storage_encoding IN ('identity', 'gzip')),
  compression TEXT NOT NULL DEFAULT 'none'
    CHECK (compression IN ('none', 'gzip', 'webp')),
  relative_path TEXT NOT NULL UNIQUE,
  original_size BIGINT NOT NULL CHECK (original_size >= 0),
  stored_size BIGINT NOT NULL CHECK (stored_size >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (transaction_id)
);

CREATE INDEX IF NOT EXISTS idx_receipts_username ON transaction_receipts(username);
CREATE INDEX IF NOT EXISTS idx_receipts_transaction ON transaction_receipts(transaction_id);
