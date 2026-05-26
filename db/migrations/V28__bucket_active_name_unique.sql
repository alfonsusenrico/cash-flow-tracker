-- Allow archived buckets to keep history without blocking reuse of the same name.

ALTER TABLE buckets
  DROP CONSTRAINT IF EXISTS buckets_user_id_name_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_buckets_user_active_name
ON buckets (user_id, lower(name))
WHERE is_archived = FALSE;
