ALTER TABLE transactions
  ADD COLUMN IF NOT EXISTS is_cycle_topup BOOLEAN NOT NULL DEFAULT FALSE;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'ck_tx_cycle_topup_debit'
  ) THEN
    ALTER TABLE transactions
      ADD CONSTRAINT ck_tx_cycle_topup_debit CHECK (NOT is_cycle_topup OR transaction_type = 'debit');
  END IF;
END $$;

UPDATE transactions
SET is_cycle_topup = TRUE
WHERE transaction_name = 'Top Up Balance'
  AND transaction_type = 'debit'
  AND is_transfer = FALSE
  AND deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_tx_cycle_topup_active
  ON transactions(account_id, date)
  WHERE deleted_at IS NULL AND is_cycle_topup = TRUE;
