ALTER TABLE accounts
  ADD COLUMN IF NOT EXISTS profile_type TEXT NOT NULL DEFAULT 'dynamic_spending'
    CHECK (profile_type IN ('tabungan', 'fixed_spending', 'dynamic_spending')),
  ADD COLUMN IF NOT EXISTS is_payroll_source BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS is_no_limit BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS is_buffer BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS fixed_limit_amount BIGINT NULL CHECK (fixed_limit_amount >= 0);

CREATE INDEX IF NOT EXISTS idx_accounts_profile_type ON accounts(profile_type);
