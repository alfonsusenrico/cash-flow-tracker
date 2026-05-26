-- V26: Account opening balances are not payday/cycle top-ups.
--
-- V6 marked historical "Top Up Balance" rows as cycle top-ups. That made
-- account-created opening balances eligible as payday anchors, which can
-- exclude the opening amount from the active summary period.
UPDATE transactions t
SET is_cycle_topup = FALSE,
    transaction_name = 'Opening Balance'
FROM accounts a
WHERE t.account_id = a.account_id
  AND t.transaction_name = 'Top Up Balance'
  AND t.transaction_type = 'debit'
  AND t.is_transfer = FALSE
  AND t.deleted_at IS NULL
  AND t.is_cycle_topup = TRUE
  AND t.category_id IS NULL
  AND COALESCE(t.notes, '') = ''
  AND ABS(EXTRACT(EPOCH FROM (t.date - a.created_at))) <= 300;
