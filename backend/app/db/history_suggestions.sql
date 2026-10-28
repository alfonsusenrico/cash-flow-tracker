-- Owner-triggered suggestions that re-enrich past rows; nothing changes a row until the owner accepts.
-- Idempotent: the identical startup mirror runs after Flyway on every API start.

CREATE TABLE IF NOT EXISTS suggestion_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    from_date DATE NOT NULL,
    to_date DATE NOT NULL,
    status VARCHAR(12) NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    max_rows INTEGER NOT NULL CHECK (max_rows > 0),
    eligible_count INTEGER NOT NULL DEFAULT 0 CHECK (eligible_count >= 0),
    selected_count INTEGER NOT NULL DEFAULT 0 CHECK (selected_count >= 0),
    processed_count INTEGER NOT NULL DEFAULT 0 CHECK (processed_count >= 0),
    memory_count INTEGER NOT NULL DEFAULT 0 CHECK (memory_count >= 0),
    rule_count INTEGER NOT NULL DEFAULT 0 CHECK (rule_count >= 0),
    ai_count INTEGER NOT NULL DEFAULT 0 CHECK (ai_count >= 0),
    unchanged_count INTEGER NOT NULL DEFAULT 0 CHECK (unchanged_count >= 0),
    rejected_before_count INTEGER NOT NULL DEFAULT 0 CHECK (rejected_before_count >= 0),
    ai_calls INTEGER NOT NULL DEFAULT 0 CHECK (ai_calls >= 0),
    error_code VARCHAR(60) NULL,
    model VARCHAR(60) NULL,
    prompt_version VARCHAR(60) NULL,
    lease_token UUID NULL,
    lease_expires_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ NULL,
    CHECK (to_date >= from_date)
);

-- One active run per owner.
CREATE UNIQUE INDEX IF NOT EXISTS uq_suggestion_runs_active
    ON suggestion_runs(user_id) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS idx_suggestion_runs_latest
    ON suggestion_runs(user_id, created_at DESC);

-- before_* columns hold the row's classification when the suggestion was made; a row that no longer
-- matches them was changed since, so its pending suggestion is superseded.
CREATE TABLE IF NOT EXISTS transaction_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    run_id UUID NULL REFERENCES suggestion_runs(id) ON DELETE SET NULL,
    proposal_source VARCHAR(10) NOT NULL CHECK (proposal_source IN ('memory', 'ai', 'rule')),
    proposed_merchant VARCHAR(120) NULL,
    proposed_notes VARCHAR(500) NULL,
    proposed_category_id UUID NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    proposed_kakeibo_type VARCHAR(20) NULL
        CHECK (proposed_kakeibo_type IS NULL OR proposed_kakeibo_type IN ('need', 'want', 'culture', 'unexpected', 'saving')),
    proposed_offset_category_id UUID NULL REFERENCES categories(id) ON DELETE CASCADE,
    confidence NUMERIC(4, 3) NULL CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    reason VARCHAR(160) NOT NULL,
    before_type VARCHAR(10) NOT NULL,
    before_merchant VARCHAR(120) NULL,
    before_notes TEXT NULL,
    before_category_id UUID NULL,
    before_kakeibo_type VARCHAR(20) NULL,
    before_offset_category_id UUID NULL,
    status VARCHAR(12) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'accepted', 'rejected', 'superseded')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decided_at TIMESTAMPTZ NULL
);

-- At most one pending suggestion per transaction.
CREATE UNIQUE INDEX IF NOT EXISTS uq_transaction_suggestions_pending
    ON transaction_suggestions(transaction_id) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_transaction_suggestions_owner
    ON transaction_suggestions(user_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_transaction_suggestions_transaction
    ON transaction_suggestions(transaction_id, status);

-- An accepted suggestion is an owner correction marked with its own kind and suggestion id.
-- Changing the constraint never updates a row, so the append-only trigger is not involved.
ALTER TABLE classification_feedback ADD COLUMN IF NOT EXISTS history_suggestion_id UUID NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'classification_feedback_kind_check'
          AND conrelid = 'classification_feedback'::regclass
          AND pg_get_constraintdef(oid) LIKE '%accepted_suggestion%'
    ) THEN
        ALTER TABLE classification_feedback DROP CONSTRAINT IF EXISTS classification_feedback_kind_check;
        ALTER TABLE classification_feedback ADD CONSTRAINT classification_feedback_kind_check
            CHECK (kind IN ('correction', 'purpose_answer', 'accepted_suggestion'));
    END IF;
END $$;
