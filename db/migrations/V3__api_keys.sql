CREATE TABLE IF NOT EXISTS api_keys (
  api_key_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
  key_hash TEXT NOT NULL UNIQUE,
  key_prefix TEXT NOT NULL,
  label TEXT NOT NULL DEFAULT 'default',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at TIMESTAMPTZ NULL,
  revoked_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_api_keys_username ON api_keys(username);
CREATE INDEX IF NOT EXISTS idx_api_keys_username_active ON api_keys(username) WHERE revoked_at IS NULL;
