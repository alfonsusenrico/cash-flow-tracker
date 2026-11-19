-- Item groups of typed captures (learn-accounts-by-item D1): what a capture phrase paid for, so the paying
-- account is learned across phrases ("parkir ptc", "parkir k24" and "parkiran mall" are all "parkir").
-- One row per owner and phrase key, written once when a capture with that key is first recorded and never
-- changed by an owner edit. `source` says who named the group: the phrase's first word, or the model.
-- Additive and repeatable. No backfill: captures recorded before this table fall back to their first word.
CREATE TABLE IF NOT EXISTS capture_item_groups (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    match_key VARCHAR(220) NOT NULL,
    item_group VARCHAR(60) NOT NULL CHECK (char_length(btrim(item_group)) BETWEEN 1 AND 60),
    source VARCHAR(10) NOT NULL CHECK (source IN ('word', 'ai')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, match_key)
);
