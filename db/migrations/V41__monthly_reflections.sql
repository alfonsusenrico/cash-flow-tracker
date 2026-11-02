-- Monthly report reflection notes ("Apa yang mau diperbaiki bulan depan?"), one per owner, basis and period.
-- Additive: dropping the table loses only the notes; no other table depends on it.
CREATE TABLE IF NOT EXISTS monthly_reflections (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    basis VARCHAR(10) NOT NULL CHECK (basis IN ('month', 'cycle')),
    -- Local (application timezone) first and last day of the reported period.
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    note VARCHAR(1000) NOT NULL CHECK (char_length(btrim(note)) BETWEEN 1 AND 1000),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (period_end >= period_start),
    UNIQUE (user_id, basis, period_start)
);
