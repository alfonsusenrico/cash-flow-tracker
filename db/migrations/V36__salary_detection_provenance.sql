-- Add provenance without changing existing owner confirmations or ledger data.
ALTER TABLE salary_receipts ADD COLUMN IF NOT EXISTS confirmation_method VARCHAR(10) NOT NULL DEFAULT 'owner'
    CHECK (confirmation_method IN ('owner', 'detected'));
ALTER TABLE salary_receipt_events ADD COLUMN IF NOT EXISTS confirmation_method VARCHAR(10) NOT NULL DEFAULT 'owner'
    CHECK (confirmation_method IN ('owner', 'detected'));
