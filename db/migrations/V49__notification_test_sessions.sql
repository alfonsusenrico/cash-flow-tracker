-- Capture-only notification test sessions ("Sesi uji", notification-test-session D1-D3, D6).
-- Additive: one table, two nullable columns, one check and two indexes; no existing row changes.
-- An older release ignores the table and the columns, so it would book notifications posted during a
-- session again; run sessions only on a release that has this migration.
-- Rollback: release first, then drop the check, the columns and the table (after deleting tagged events
-- if wanted). Every statement is safe to run twice.

-- 1. A session is a time window per owner. `data_deleted_at` marks "Hapus data sesi": the window stays,
--    so a late upload of a notification posted during it is still captured without booking.
CREATE TABLE IF NOT EXISTS notification_test_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    started_at TIMESTAMPTZ NOT NULL,
    planned_end_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ NULL,
    data_deleted_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT notification_test_sessions_window CHECK (planned_end_at > started_at),
    CONSTRAINT notification_test_sessions_end CHECK (ended_at IS NULL OR ended_at >= started_at)
);

-- At most one open session per owner.
CREATE UNIQUE INDEX IF NOT EXISTS notification_test_sessions_one_open
    ON notification_test_sessions (user_id) WHERE ended_at IS NULL;
CREATE INDEX IF NOT EXISTS notification_test_sessions_window_lookup
    ON notification_test_sessions (user_id, started_at);

-- 2. The session a notification was captured for, and the owner's case number for the replay.
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS test_session_id UUID NULL
    REFERENCES notification_test_sessions(id) ON DELETE CASCADE;
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS test_case TEXT NULL;
CREATE INDEX IF NOT EXISTS notification_events_test_session
    ON notification_events (user_id, test_session_id) WHERE test_session_id IS NOT NULL;

-- 3. A tagged event never books: any later state change or booking fails instead of slipping through.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'notification_events_test_session_capture_only'
    ) THEN
        ALTER TABLE notification_events ADD CONSTRAINT notification_events_test_session_capture_only
            CHECK (test_session_id IS NULL
                   OR (processing_state = 'ignored' AND transaction_id IS NULL AND movement_id IS NULL));
    END IF;
END $$;
