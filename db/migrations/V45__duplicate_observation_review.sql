-- One arrival seen by two apps (recognise-one-arrival-seen-twice D3, D6).
-- Additive and idempotent: two nullable columns, two partial indexes and a widened kind check. An older
-- release ignores the columns and never writes the new kind. Rollback: leave them in place, unused.

-- 1. When the owner acknowledged a second observation ("Benar, satu transaksi") or a held possible
--    duplicate; NULL while it waits in Tinjau's "Terlihat dua kali".
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ NULL;

-- 2. The leg a second observation reports. Kept apart from transaction_id, which stays one event per
--    transaction for match keys, habits and feedback; deleting the leg leaves the event without a link.
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS duplicate_of_transaction_id UUID NULL
    REFERENCES transactions(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS notification_events_duplicate_of_transaction
    ON notification_events (duplicate_of_transaction_id) WHERE duplicate_of_transaction_id IS NOT NULL;

-- 3. The Tinjau section reads the owner's unacknowledged duplicate observations by time.
CREATE INDEX IF NOT EXISTS notification_events_unreviewed_duplicates
    ON notification_events (user_id, post_time)
    WHERE provenance_kind = 'duplicate_observation' AND reviewed_at IS NULL;

-- 4. "Catat terpisah" is an undoable review action.
ALTER TABLE review_actions DROP CONSTRAINT IF EXISTS review_actions_kind_check;
ALTER TABLE review_actions ADD CONSTRAINT review_actions_kind_check CHECK (kind IN (
    'review_accept', 'review_recategorise', 'review_rename', 'review_keep_separate',
    'movement_complete', 'movement_merge', 'topup_conversion', 'topup_rejection',
    'purpose_answer', 'suggestion_accept', 'suggestion_reject', 'seen_twice_separate'));
