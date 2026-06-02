-- Create chat history table for threaded conversation context
CREATE TABLE IF NOT EXISTS bot_chat_history (
  history_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  telegram_user_id  BIGINT NOT NULL REFERENCES bot_users(telegram_user_id) ON DELETE CASCADE,
  role              TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
  content           TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bot_chat_history_user
  ON bot_chat_history(telegram_user_id, created_at ASC);
