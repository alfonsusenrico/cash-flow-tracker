ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS active_question JSONB;
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS sender_resolution JSONB;

CREATE UNIQUE INDEX IF NOT EXISTS uq_accounts_owner_id ON accounts(user_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_notification_events_owner_id ON notification_events(user_id, id);

CREATE TABLE IF NOT EXISTS notification_sender_aliases (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    institution VARCHAR(30) NOT NULL,
    receiving_account_id UUID NOT NULL,
    mask_normalized VARCHAR(150) NOT NULL,
    mask_display VARCHAR(150) NOT NULL,
    sender_name VARCHAR(80) NOT NULL CHECK (length(trim(sender_name)) > 0),
    state VARCHAR(10) NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'ambiguous')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (user_id, receiving_account_id) REFERENCES accounts(user_id, id) ON DELETE CASCADE,
    UNIQUE (user_id, institution, receiving_account_id, mask_normalized)
);

CREATE TABLE IF NOT EXISTS notification_sender_answers (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    event_id UUID NOT NULL,
    question_id UUID NOT NULL,
    reply_id UUID NOT NULL,
    action VARCHAR(10) NOT NULL CHECK (action IN ('name', 'unknown')),
    sender_name VARCHAR(80),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, question_id),
    FOREIGN KEY (user_id, event_id) REFERENCES notification_events(user_id, id) ON DELETE CASCADE,
    UNIQUE (user_id, reply_id),
    CHECK ((action = 'name' AND sender_name IS NOT NULL AND length(trim(sender_name)) > 0)
        OR (action = 'unknown' AND sender_name IS NULL))
);
