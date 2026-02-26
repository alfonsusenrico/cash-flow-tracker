CREATE TABLE IF NOT EXISTS internal_loans (
  loan_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
  trigger_transaction_id UUID NOT NULL REFERENCES transactions(transaction_id) ON DELETE CASCADE,
  disbursement_transfer_id UUID NOT NULL UNIQUE,
  lender_account_id UUID NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
  borrower_account_id UUID NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
  principal_amount BIGINT NOT NULL CHECK (principal_amount > 0),
  status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'finalized')),
  finalized_transfer_id UUID NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finalized_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_internal_loans_username_status
  ON internal_loans(username, status);

CREATE INDEX IF NOT EXISTS idx_internal_loans_trigger_tx
  ON internal_loans(trigger_transaction_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_internal_loans_trigger_open
  ON internal_loans(trigger_transaction_id)
  WHERE status = 'open';
