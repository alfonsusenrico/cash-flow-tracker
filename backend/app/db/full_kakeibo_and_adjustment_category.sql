-- Full Kakeibo pillar set and the excluded system category for reconcile adjustments.
-- Idempotent: the API runs an identical copy at startup (backend/app/db/full_kakeibo_and_adjustment_category.sql).

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'transactions'::regclass
          AND conname = 'transactions_kakeibo_type_check'
          AND pg_get_constraintdef(oid) LIKE '%culture%'
    ) THEN
        ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_kakeibo_type_check;
        ALTER TABLE transactions ADD CONSTRAINT transactions_kakeibo_type_check
            CHECK (kakeibo_type IN ('need', 'want', 'culture', 'unexpected', 'saving'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'categories'::regclass
          AND conname = 'categories_kakeibo_type_check'
          AND pg_get_constraintdef(oid) LIKE '%culture%'
    ) THEN
        ALTER TABLE categories DROP CONSTRAINT IF EXISTS categories_kakeibo_type_check;
        ALTER TABLE categories ADD CONSTRAINT categories_kakeibo_type_check
            CHECK (kakeibo_type IN ('need', 'want', 'culture', 'unexpected', 'saving'));
    END IF;
END $$;

-- Adjustments change balances but are neither spending nor income, so the category is excluded
-- from budget reports and carries no pillar.
INSERT INTO categories (user_id, name, icon, color, kind, kakeibo_type, is_excluded_from_budget)
SELECT u.id, 'Penyesuaian Saldo', 'wallet', '#64748b', kinds.kind, NULL, TRUE
FROM users u
CROSS JOIN (VALUES ('expense'), ('income')) AS kinds(kind)
ON CONFLICT (user_id, name, kind) DO UPDATE
SET is_excluded_from_budget = TRUE, kakeibo_type = NULL, is_archived = FALSE;

-- Only rows written by the reconcile endpoint with its default note are identifiable; updated_at
-- is left alone because this is not an owner edit.
UPDATE transactions t
SET category_id = c.id
FROM categories c
WHERE c.user_id = t.user_id
  AND c.name = 'Penyesuaian Saldo'
  AND c.kind = t.type
  AND t.category_id IS NULL
  AND t.movement_id IS NULL
  AND t.idempotency_key IS NULL
  AND t.notes = 'Balance Adjustment (Reconciliation)';
