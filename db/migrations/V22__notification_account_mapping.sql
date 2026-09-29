-- Learned mappings from bank-notification names to the owner's accounts.
CREATE TABLE IF NOT EXISTS notification_account_aliases (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    institution VARCHAR(30) NOT NULL,
    name_normalized VARCHAR(150) NOT NULL,
    name_display VARCHAR(150) NOT NULL,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    source VARCHAR(10) NOT NULL CHECK (source IN ('ai', 'owner')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_notification_account_alias UNIQUE (user_id, institution, name_normalized)
);

-- Events whose account mapping awaits the owner's confirmation.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'notification_events'::regclass AND conname = 'chk_notification_processing_state'
          AND pg_get_constraintdef(oid) LIKE '%needs_confirmation%'
    ) THEN
        ALTER TABLE notification_events DROP CONSTRAINT IF EXISTS chk_notification_processing_state;
        ALTER TABLE notification_events ADD CONSTRAINT chk_notification_processing_state
            CHECK (processing_state IN ('queued', 'processing', 'recorded', 'needs_review', 'needs_confirmation', 'ignored', 'failed'));
    END IF;
END $$;
