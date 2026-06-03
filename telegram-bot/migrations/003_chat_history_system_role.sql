-- Allow 'system' role in bot_chat_history
ALTER TABLE bot_chat_history DROP CONSTRAINT bot_chat_history_role_check;
ALTER TABLE bot_chat_history ADD CONSTRAINT bot_chat_history_role_check CHECK (role IN ('user', 'assistant', 'system'));
