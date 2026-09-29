-- Owner name renderings used to recognise self-transfers in bank notifications.
ALTER TABLE users ADD COLUMN IF NOT EXISTS name_aliases TEXT[] NOT NULL DEFAULT '{}';

-- Failed notification events are retried automatically on an escalating schedule.
CREATE INDEX IF NOT EXISTS idx_notifications_failed_retry
    ON notification_events(next_attempt_at) WHERE processing_state = 'failed';
