-- QR device pairing: one revocable API key per paired phone, and single-use pairing codes.
-- Additive: the old code ignores the new columns and table (it would accept revoked keys again).

-- 1. Every existing key was created by hand in web Settings or at registration, so it is 'manual'.
ALTER TABLE api_keys
    ADD COLUMN IF NOT EXISTS device_name VARCHAR(80) NULL,
    ADD COLUMN IF NOT EXISTS device_id VARCHAR(64) NULL,
    ADD COLUMN IF NOT EXISTS created_via VARCHAR(10) NOT NULL DEFAULT 'manual',
    ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chk_api_keys_created_via' AND conrelid = 'api_keys'::regclass
    ) THEN
        ALTER TABLE api_keys ADD CONSTRAINT chk_api_keys_created_via CHECK (
            (created_via = 'manual' AND device_id IS NULL)
            OR (created_via = 'pairing' AND device_id IS NOT NULL AND device_name IS NOT NULL)
        );
    END IF;
END $$;

-- Re-pairing a phone replaces its key, so a device holds at most one live key per owner.
CREATE UNIQUE INDEX IF NOT EXISTS uq_api_keys_user_device
    ON api_keys (user_id, device_id)
    WHERE device_id IS NOT NULL AND revoked_at IS NULL;

-- 2. Pairing codes are stored only as SHA-256 hashes. A code is live while unused, not
--    invalidated by a newer code, unexpired and below the failed-attempt limit.
CREATE TABLE IF NOT EXISTS pairing_codes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    code_hash VARCHAR(64) NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ NULL,
    invalidated_at TIMESTAMPTZ NULL,
    failed_attempts SMALLINT NOT NULL DEFAULT 0 CHECK (failed_attempts >= 0),
    api_key_id UUID NULL REFERENCES api_keys(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pairing_codes_user_id ON pairing_codes (user_id);
CREATE INDEX IF NOT EXISTS idx_pairing_codes_live
    ON pairing_codes (expires_at)
    WHERE used_at IS NULL AND invalidated_at IS NULL;
