-- Server-side undo of Tinjau review actions (redesign-review-triage D10, D13).
-- Additive and idempotent: one new table, one nullable column, a widened kind check, a new check and
-- three indexes. Older code ignores all of it and never writes an `undo` feedback row.

-- 1. One record per undoable review action. Only the sha256 of the token is stored, so a database read
--    cannot reuse a token. The snapshot holds before and after images and is cleared once the token is
--    used or expires; records are deleted 24 hours after the action.
CREATE TABLE IF NOT EXISTS review_actions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash BYTEA NOT NULL UNIQUE CHECK (octet_length(token_hash) = 32),
    kind VARCHAR(30) NOT NULL CHECK (kind IN (
        'review_accept', 'review_recategorise', 'review_rename', 'review_keep_separate',
        'movement_complete', 'movement_merge', 'topup_conversion', 'topup_rejection',
        'purpose_answer', 'suggestion_accept', 'suggestion_reject')),
    -- No foreign key: the record must survive a deleted row, so a later undo can name the conflict.
    transaction_ids UUID[] NOT NULL,
    snapshot JSONB NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    undone_at TIMESTAMPTZ NULL,
    CHECK (expires_at > created_at),
    CHECK (undone_at IS NULL OR snapshot IS NULL)
);

CREATE INDEX IF NOT EXISTS idx_review_actions_expiry
    ON review_actions (expires_at) WHERE snapshot IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_review_actions_owner
    ON review_actions (user_id, created_at DESC);

-- 2. An `undo` feedback row names the row it reverts. No foreign key, following V29: a labelled example
--    outlives what it describes. ADD COLUMN with a NULL default rewrites nothing.
ALTER TABLE classification_feedback ADD COLUMN IF NOT EXISTS reverts_feedback_id UUID NULL;

-- Changing the constraint never updates a row, so the append-only trigger is not involved.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'classification_feedback_kind_check'
          AND conrelid = 'classification_feedback'::regclass
          AND pg_get_constraintdef(oid) LIKE '%''undo''%'
    ) THEN
        ALTER TABLE classification_feedback DROP CONSTRAINT IF EXISTS classification_feedback_kind_check;
        ALTER TABLE classification_feedback ADD CONSTRAINT classification_feedback_kind_check
            CHECK (kind IN ('correction', 'purpose_answer', 'accepted_suggestion', 'capture', 'undo'));
    END IF;
END $$;

-- Every existing row holds (none is `undo` and the column is new), so the check is validated after it is
-- added without blocking writes while it is checked.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'classification_feedback_undo_reverts_check'
          AND conrelid = 'classification_feedback'::regclass
    ) THEN
        ALTER TABLE classification_feedback ADD CONSTRAINT classification_feedback_undo_reverts_check
            CHECK ((kind = 'undo') = (reverts_feedback_id IS NOT NULL)) NOT VALID;
    END IF;
END $$;
ALTER TABLE classification_feedback VALIDATE CONSTRAINT classification_feedback_undo_reverts_check;

-- A label is reverted at most once.
CREATE UNIQUE INDEX IF NOT EXISTS idx_classification_feedback_reverts_once
    ON classification_feedback (reverts_feedback_id) WHERE reverts_feedback_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_classification_feedback_reverts
    ON classification_feedback (user_id, reverts_feedback_id) WHERE reverts_feedback_id IS NOT NULL;
