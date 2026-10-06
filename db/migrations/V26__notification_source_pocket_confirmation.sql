ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS source_pocket_resolution JSONB;

CREATE TABLE IF NOT EXISTS notification_source_pocket_answers (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    event_id UUID NOT NULL,
    question_id UUID NOT NULL,
    reply_id UUID NOT NULL,
    account_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, question_id),
    FOREIGN KEY (user_id, event_id) REFERENCES notification_events(user_id, id) ON DELETE CASCADE,
    UNIQUE (user_id, reply_id)
);
