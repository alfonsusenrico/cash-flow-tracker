ALTER TABLE api_keys
  ADD COLUMN IF NOT EXISTS key_masked TEXT;

UPDATE api_keys
SET key_masked = COALESCE(key_masked, key_prefix || '***')
WHERE key_masked IS NULL;

WITH ranked AS (
  SELECT api_key_id,
         ROW_NUMBER() OVER (PARTITION BY username ORDER BY created_at DESC, api_key_id DESC) AS rn
  FROM api_keys
  WHERE revoked_at IS NULL
)
UPDATE api_keys k
SET revoked_at = now()
FROM ranked r
WHERE k.api_key_id = r.api_key_id
  AND r.rn > 1;

DROP INDEX IF EXISTS idx_api_keys_username_active;

CREATE UNIQUE INDEX IF NOT EXISTS uq_api_keys_one_active_per_user
ON api_keys(username)
WHERE revoked_at IS NULL;
