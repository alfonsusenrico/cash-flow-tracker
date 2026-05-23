-- Phase 0: Drop unused opening_balance column.
-- Balance is always derived from transactions; this column was never written
-- after account creation and caused compute_financial_safety_report to diverge.
ALTER TABLE accounts DROP COLUMN IF EXISTS opening_balance;
