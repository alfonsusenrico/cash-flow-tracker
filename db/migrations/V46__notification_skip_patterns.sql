-- Redundant notifications skipped by owner patterns (skip-redundant-notifications D2, D6).
-- Additive: one table, one nullable column with a partial index, and a one-time requeue. An older
-- release ignores the table and the column; a skipped event stays an acknowledged duplicate
-- observation to it. Rollback: revert the release and leave the table and column in place, unused.

-- 1. "Reports from one app that repeat another app's report": the redundant app's reports in one
--    direction are skipped when the kept app has already reported the same money. Institutions, not
--    packages (OD123); a stopped pattern never matches.
CREATE TABLE IF NOT EXISTS notification_skip_patterns (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    redundant_institution TEXT NOT NULL
        CHECK (redundant_institution IN ('bca', 'jago', 'gopay', 'shopeepay', 'stockbit')),
    direction TEXT NOT NULL CHECK (direction IN ('income', 'expense')),
    kept_institution TEXT NOT NULL
        CHECK (kept_institution IN ('bca', 'jago', 'gopay', 'shopeepay', 'stockbit')),
    source TEXT NOT NULL CHECK (source IN ('confirmation', 'manual')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    stopped_at TIMESTAMPTZ NULL,
    CONSTRAINT notification_skip_patterns_two_apps CHECK (redundant_institution <> kept_institution)
);
CREATE UNIQUE INDEX IF NOT EXISTS notification_skip_patterns_one_active
    ON notification_skip_patterns (user_id, redundant_institution, direction, kept_institution)
    WHERE stopped_at IS NULL;

-- 2. The pattern that skipped an event; NULL for every other event, and when the pattern is deleted.
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS skip_pattern_id UUID NULL
    REFERENCES notification_skip_patterns(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS notification_events_skipped
    ON notification_events (user_id, post_time) WHERE skip_pattern_id IS NOT NULL;

-- 3. Events the retired model-only hold left waiting go back to the processor, which decides them
--    with the deterministic guard and the patterns (OD126). The same reset confirm_mapping performs;
--    a second run finds nothing to change.
UPDATE notification_events
SET processing_state = 'queued', attempt_count = 0, processing_generation = processing_generation + 1,
    next_attempt_at = NOW(), error_code = NULL, interpretation = NULL, active_question = NULL,
    lease_token = NULL, lease_expires_at = NULL, updated_at = NOW()
WHERE processing_state = 'needs_review' AND error_code = 'possible_duplicate_observation';
