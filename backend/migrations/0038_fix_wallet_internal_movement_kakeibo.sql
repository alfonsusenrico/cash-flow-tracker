-- 0038_fix_wallet_internal_movement_kakeibo.sql
-- Ensure internal movements from wallet accounts (which use type 'wallet') have kakeibo_type cleared.

UPDATE transactions t
SET kakeibo_type = NULL
FROM accounts a
WHERE t.account_id = a.id
  AND t.category_id IN (SELECT id FROM categories WHERE name = 'Internal Movement')
  AND t.goal_id IS NULL
  AND (
    a.type IN ('cash', 'bank', 'wallet', 'ewallet', 'regular')
    OR a.type IS NULL
  )
  AND (
    t.notes IS NULL
    OR t.notes = ''
    OR t.notes ILIKE '%payment%'
    OR t.notes ILIKE '%internal movement%'
    OR t.notes ILIKE '%pindah saldo%'
  );
