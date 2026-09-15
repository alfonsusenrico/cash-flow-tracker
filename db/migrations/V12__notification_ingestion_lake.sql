-- V12__notification_ingestion_lake.sql
-- Notification Ingestion Lake for Mobile Notification Companion App

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
    is_financial BOOLEAN NULL,
    event_class VARCHAR(50) NULL,
    expected_amount NUMERIC(15, 2) NULL,
    expected_direction VARCHAR(20) NULL,
    expected_counterparty VARCHAR(255) NULL,
    label_notes TEXT NULL,
    labelled_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_notification_user_hash UNIQUE (user_id, payload_hash)
);

CREATE INDEX IF NOT EXISTS idx_notifications_user_post_time ON notification_events(user_id, post_time DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_user_package ON notification_events(user_id, package_name);
CREATE INDEX IF NOT EXISTS idx_notifications_user_class ON notification_events(user_id, event_class) WHERE event_class IS NOT NULL;
