-- Latest companion health report per device key (device-health capability): no history, no personal data.
-- Additive: older code ignores the table; dropping it loses only the latest status of each phone.
CREATE TABLE IF NOT EXISTS device_health (
    api_key_id UUID PRIMARY KEY REFERENCES api_keys(id) ON DELETE CASCADE,
    app_version VARCHAR(32) NOT NULL CHECK (app_version ~ '^[0-9A-Za-z.+_-]{1,32}$'),
    listener_connected BOOLEAN NOT NULL,
    notification_access BOOLEAN NOT NULL,
    post_notifications BOOLEAN NOT NULL,
    battery_unrestricted BOOLEAN NOT NULL,
    self_test_status VARCHAR(8) NOT NULL CHECK (self_test_status IN ('passed', 'failed', 'not_run')),
    self_test_failed_step VARCHAR(8) NULL CHECK (self_test_failed_step IN ('post', 'capture', 'server')),
    -- Phone clock, shown as reported; staleness is judged by the server's reported_at.
    self_test_at TIMESTAMPTZ NULL,
    last_capture_at TIMESTAMPTZ NULL,
    queue_pending INTEGER NOT NULL CHECK (queue_pending >= 0),
    reported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Only a failed self-test names its step, and only a self-test that ran has a time.
    CHECK ((self_test_status = 'failed') = (self_test_failed_step IS NOT NULL)),
    CHECK (self_test_status = 'not_run' OR self_test_at IS NOT NULL)
);
