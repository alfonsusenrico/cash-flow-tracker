-- Phase 2: Buckets — named pockets of money earmarked for a purpose.
-- kind: spending | sinking | emergency | goal | investment
-- linked_account_id: optional physical account this bucket draws from

CREATE TABLE IF NOT EXISTS buckets (
  bucket_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id           UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  name              TEXT NOT NULL,
  kind              TEXT NOT NULL DEFAULT 'spending'
                      CHECK (kind IN ('spending','sinking','emergency','goal','investment')),
  target_amount     BIGINT NULL CHECK (target_amount >= 0),
  linked_account_id UUID NULL REFERENCES accounts(account_id) ON DELETE SET NULL,
  priority          INT NOT NULL DEFAULT 50,
  is_archived       BOOLEAN NOT NULL DEFAULT FALSE,
  notes             TEXT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (user_id, name)
);

CREATE INDEX IF NOT EXISTS idx_buckets_user_id ON buckets(user_id);
CREATE INDEX IF NOT EXISTS idx_buckets_user_kind ON buckets(user_id, kind) WHERE is_archived = FALSE;

-- Seed one emergency bucket per existing user
INSERT INTO buckets (user_id, name, kind, priority)
SELECT user_id, 'Emergency Fund', 'emergency', 10
FROM users
ON CONFLICT (user_id, name) DO NOTHING;
