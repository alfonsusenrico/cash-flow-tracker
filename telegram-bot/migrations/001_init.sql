-- Bot state: linked users (encrypted API keys) and durable pending confirmations.
-- Kept in its own tables so the bot DB can be split to a separate instance later.

CREATE TABLE IF NOT EXISTS bot_users (
  telegram_user_id  BIGINT PRIMARY KEY,
  api_key_encrypted BYTEA NOT NULL,
  username_hint     TEXT NULL,
  linked_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at      TIMESTAMPTZ NULL
);

CREATE TABLE IF NOT EXISTS bot_pending_actions (
  pending_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  telegram_user_id  BIGINT NOT NULL REFERENCES bot_users(telegram_user_id) ON DELETE CASCADE,
  chat_id           BIGINT NOT NULL,
  action_json       JSONB NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at        TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bot_pending_user
  ON bot_pending_actions(telegram_user_id, created_at DESC);
