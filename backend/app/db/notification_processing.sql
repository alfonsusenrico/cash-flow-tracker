CREATE TABLE IF NOT EXISTS notification_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id VARCHAR(64) NOT NULL,
    package_name VARCHAR(255) NOT NULL,
    app_label VARCHAR(100),
    notification_key TEXT,
    notification_id INT,
    channel_id VARCHAR(100),
    category VARCHAR(50),
    title TEXT,
    body_text TEXT,
    big_text TEXT,
    sub_text TEXT,
    summary_text TEXT,
    post_time TIMESTAMPTZ NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload_hash VARCHAR(64) NOT NULL,
    raw_extras JSONB,
    source_version VARCHAR(50),
    is_financial BOOLEAN,
    event_class VARCHAR(50),
    expected_amount NUMERIC(15, 2),
    expected_direction VARCHAR(20),
    expected_counterparty VARCHAR(255),
    label_notes TEXT,
    labelled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_notification_user_hash UNIQUE (user_id, payload_hash)
);
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS parsed_summary JSONB;
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS transaction_id UUID REFERENCES transactions(id) ON DELETE SET NULL;
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS processing_state VARCHAR(20);
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS processing_mode VARCHAR(20);
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS attempt_count INT NOT NULL DEFAULT 0;
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS processing_generation INT NOT NULL DEFAULT 0;
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS lease_token UUID;
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS error_code VARCHAR(100);
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS provider VARCHAR(30);
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS model VARCHAR(100);
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS prompt_version VARCHAR(100);
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS captured_facts JSONB;
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS interpretation JSONB;
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS movement_id UUID;
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS provenance_kind VARCHAR(30);
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS confirmed_role VARCHAR(10);
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS result_key VARCHAR(100);
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS result_snapshot JSONB;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'notification_events'::regclass AND conname = 'chk_notification_processing_state') THEN
        ALTER TABLE notification_events ADD CONSTRAINT chk_notification_processing_state
            CHECK (processing_state IN ('queued', 'processing', 'recorded', 'needs_review', 'ignored', 'failed'));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'notification_events'::regclass AND conname = 'chk_notification_processing_attempts') THEN
        ALTER TABLE notification_events ADD CONSTRAINT chk_notification_processing_attempts CHECK (attempt_count >= 0 AND processing_generation >= 0);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'notification_events'::regclass AND conname = 'chk_notification_recorded_result') THEN
        ALTER TABLE notification_events ADD CONSTRAINT chk_notification_recorded_result
            CHECK (processing_state <> 'recorded' OR (result_key IS NOT NULL AND result_snapshot IS NOT NULL));
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'notification_events'::regclass AND conname = 'chk_notification_confirmed_role') THEN
        ALTER TABLE notification_events ADD CONSTRAINT chk_notification_confirmed_role
            CHECK (confirmed_role IN ('outbound', 'inbound'));
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_notifications_processing_queue
    ON notification_events(next_attempt_at, created_at) WHERE processing_state IN ('queued', 'processing');
CREATE INDEX IF NOT EXISTS idx_notifications_movement_candidates
    ON notification_events(user_id, post_time, transaction_id) WHERE processing_state = 'recorded';
CREATE UNIQUE INDEX IF NOT EXISTS uq_notification_confirmed_movement_role
    ON notification_events(user_id, movement_id, confirmed_role)
    WHERE movement_id IS NOT NULL AND confirmed_role IS NOT NULL;
CREATE TABLE IF NOT EXISTS merchant_category_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    merchant_pattern VARCHAR(255) NOT NULL,
    category_id UUID NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_user_merchant_rule UNIQUE (user_id, merchant_pattern)
);
