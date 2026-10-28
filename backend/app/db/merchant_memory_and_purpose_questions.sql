-- Merchant memory learned from owner corrections, and append-only labelled examples.
-- Idempotent: the identical startup mirror runs after Flyway on every API start.

CREATE TABLE IF NOT EXISTS merchant_memory (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    match_key VARCHAR(220) NOT NULL CHECK (length(match_key) > 0),
    key_kind VARCHAR(10) NOT NULL CHECK (key_kind IN ('merchant', 'signature')),
    display_merchant VARCHAR(120) NULL,
    notes VARCHAR(500) NULL,
    category_id UUID NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    kakeibo_type VARCHAR(20) NULL
        CHECK (kakeibo_type IS NULL OR kakeibo_type IN ('need', 'want', 'culture', 'unexpected', 'saving')),
    offset_category_id UUID NULL REFERENCES categories(id) ON DELETE SET NULL,
    source VARCHAR(10) NOT NULL CHECK (source IN ('owner', 'ai')),
    hit_count INTEGER NOT NULL DEFAULT 0 CHECK (hit_count >= 0),
    last_used_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, match_key)
);

-- No foreign keys to transactions, events or categories: an ON DELETE SET NULL would rewrite a
-- labelled example in place, and the example must outlive the row it describes.
CREATE TABLE IF NOT EXISTS classification_feedback (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    kind VARCHAR(20) NOT NULL CHECK (kind IN ('correction', 'purpose_answer')),
    transaction_id UUID NULL,
    event_id UUID NULL,
    question_id UUID NULL,
    reply_id UUID NULL,
    suggestion_id VARCHAR(16) NULL,
    answer_text VARCHAR(200) NULL,
    before_merchant VARCHAR(120) NULL,
    before_notes VARCHAR(500) NULL,
    before_category_id UUID NULL,
    before_category_name VARCHAR(100) NULL,
    before_kakeibo_type VARCHAR(20) NULL,
    before_type VARCHAR(10) NULL,
    before_offset_category_id UUID NULL,
    after_merchant VARCHAR(120) NULL,
    after_notes VARCHAR(500) NULL,
    after_category_id UUID NULL,
    after_category_name VARCHAR(100) NULL,
    after_kakeibo_type VARCHAR(20) NULL,
    after_type VARCHAR(10) NULL,
    after_offset_category_id UUID NULL,
    local_weekday SMALLINT NULL CHECK (local_weekday BETWEEN 1 AND 7),
    local_hour SMALLINT NULL CHECK (local_hour BETWEEN 0 AND 23),
    amount BIGINT NULL,
    account_id UUID NULL,
    institution VARCHAR(30) NULL,
    text_signature VARCHAR(200) NULL,
    match_key VARCHAR(220) NULL,
    transaction_source VARCHAR(20) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, reply_id),
    CHECK (kind <> 'purpose_answer' OR (question_id IS NOT NULL AND reply_id IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_classification_feedback_examples
    ON classification_feedback(user_id, after_type, created_at DESC);

CREATE OR REPLACE FUNCTION reject_classification_feedback_update() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'classification_feedback is append-only';
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'classification_feedback_append_only'
          AND tgrelid = 'classification_feedback'::regclass
    ) THEN
        CREATE TRIGGER classification_feedback_append_only
            BEFORE UPDATE ON classification_feedback
            FOR EACH ROW EXECUTE FUNCTION reject_classification_feedback_update();
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_notification_events_open_purpose
    ON notification_events(user_id, post_time DESC)
    WHERE active_question ->> 'type' = 'purpose';
