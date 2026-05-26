-- Allow one bucket to be backed by multiple physical accounts.
-- The legacy buckets.linked_account_id column is kept for compatibility with
-- older clients and mirrors the first linked account when writes come through
-- the current API.

CREATE TABLE IF NOT EXISTS bucket_accounts (
  bucket_id  UUID NOT NULL REFERENCES buckets(bucket_id) ON DELETE CASCADE,
  account_id UUID NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (bucket_id, account_id)
);

CREATE INDEX IF NOT EXISTS idx_bucket_accounts_account ON bucket_accounts(account_id);

INSERT INTO bucket_accounts (bucket_id, account_id)
SELECT bucket_id, linked_account_id
FROM buckets
WHERE linked_account_id IS NOT NULL
ON CONFLICT DO NOTHING;
