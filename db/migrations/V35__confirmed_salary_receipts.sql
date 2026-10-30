-- Additive salary source and receipt confirmation; historical ledger rows are untouched.
CREATE TABLE IF NOT EXISTS salary_sources (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    matcher_field VARCHAR(10) NOT NULL CHECK (matcher_field IN ('merchant', 'notes')),
    matcher_text TEXT NOT NULL CHECK (length(matcher_text) BETWEEN 1 AND 500),
    configured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS salary_receipts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    transaction_id UUID REFERENCES transactions(id) ON DELETE SET NULL,
    confirmed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ,
    UNIQUE (user_id, transaction_id)
);

CREATE TABLE IF NOT EXISTS salary_source_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    transaction_id UUID REFERENCES transactions(id) ON DELETE SET NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS salary_receipt_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    salary_receipt_id UUID NOT NULL REFERENCES salary_receipts(id) ON DELETE CASCADE,
    action VARCHAR(10) NOT NULL CHECK (action IN ('confirm', 'revoke')),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_salary_receipts_user_active ON salary_receipts (user_id)
    WHERE revoked_at IS NULL AND transaction_id IS NOT NULL;
