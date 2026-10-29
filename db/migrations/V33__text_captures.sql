-- Text capture: transactions the owner types as one short sentence on the phone.
-- Additive: the old code never writes source 'capture', feedback kind 'capture' or a memory account.

-- 1. Transactions recorded from typed text carry their own source.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chk_transactions_source'
          AND conrelid = 'transactions'::regclass
          AND pg_get_constraintdef(oid) LIKE '%capture%'
    ) THEN
        ALTER TABLE transactions DROP CONSTRAINT IF EXISTS chk_transactions_source;
        ALTER TABLE transactions ADD CONSTRAINT chk_transactions_source CHECK (
            source IN ('manual', 'notification', 'recurring', 'payroll', 'movement',
                       'reconcile', 'investment', 'import', 'capture')
        );
    END IF;
END $$;

-- 2. A recorded capture appends one labelled example of its own kind. Changing the constraint
--    never updates a row, so the append-only trigger is not involved.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'classification_feedback_kind_check'
          AND conrelid = 'classification_feedback'::regclass
          AND pg_get_constraintdef(oid) LIKE '%capture%'
    ) THEN
        ALTER TABLE classification_feedback DROP CONSTRAINT IF EXISTS classification_feedback_kind_check;
        ALTER TABLE classification_feedback ADD CONSTRAINT classification_feedback_kind_check
            CHECK (kind IN ('correction', 'purpose_answer', 'accepted_suggestion', 'capture'));
    END IF;
END $$;

-- 3. The account the owner pays a remembered merchant with; only captures write it.
ALTER TABLE merchant_memory
    ADD COLUMN IF NOT EXISTS account_id UUID NULL REFERENCES accounts(id) ON DELETE SET NULL;

-- 4. One row per typed capture. The text is the owner's own note; logs never carry it.
CREATE TABLE IF NOT EXISTS text_captures (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    client_capture_id UUID NOT NULL,
    text VARCHAR(200) NOT NULL CHECK (char_length(btrim(text)) BETWEEN 1 AND 200),
    device_time TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('needs_detail', 'recorded', 'undone')),
    match_key VARCHAR(220) NULL,
    draft JSONB NOT NULL DEFAULT '{}'::jsonb,
    question JSONB NULL,
    answers JSONB NOT NULL DEFAULT '[]'::jsonb,
    transaction_id UUID NULL REFERENCES transactions(id) ON DELETE SET NULL,
    movement_id UUID NULL,
    recorded_rows JSONB NULL,
    recorded_at TIMESTAMPTZ NULL,
    undo_until TIMESTAMPTZ NULL,
    undone_at TIMESTAMPTZ NULL,
    classifier VARCHAR(40) NULL,
    model VARCHAR(50) NULL,
    prompt_version VARCHAR(50) NULL,
    model_calls SMALLINT NOT NULL DEFAULT 0 CHECK (model_calls BETWEEN 0 AND 2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_text_captures_client UNIQUE (user_id, client_capture_id),
    CONSTRAINT chk_text_captures_question CHECK ((status = 'needs_detail') = (question IS NOT NULL)),
    CONSTRAINT chk_text_captures_recorded CHECK (status = 'needs_detail' OR recorded_at IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_text_captures_user_key
    ON text_captures(user_id, match_key, recorded_at DESC) WHERE status = 'recorded';
