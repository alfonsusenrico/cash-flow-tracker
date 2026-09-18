-- V16__fix_internal_movement_kakeibo.sql
-- Decouple 'Internal Movement' category from Kakeibo 'saving' classification
-- and reset misclassified liquid transfers and stock trades.

-- 1. Ensure 'Internal Movement' category has no kakeibo_type default
UPDATE categories
SET kakeibo_type = NULL
WHERE name = 'Internal Movement';

-- 2. Clear kakeibo_type for stock trade movements
UPDATE transactions
SET kakeibo_type = NULL
WHERE category_id IN (SELECT id FROM categories WHERE name = 'Internal Movement')
  AND (notes ILIKE '%lot @%' OR notes ILIKE '%stockbit%');

-- 3. Clear kakeibo_type for card verification pre-auth holds and reversals
UPDATE transactions
SET kakeibo_type = NULL
WHERE category_id IN (SELECT id FROM categories WHERE name = 'Internal Movement')
  AND (notes ILIKE '%pre-auth%' OR notes ILIKE '%verifikasi kartu%');

-- 4. Clear kakeibo_type for liquid inter-account / pocket transfers
UPDATE transactions t
SET kakeibo_type = NULL
FROM accounts a
WHERE t.account_id = a.id
  AND t.category_id IN (SELECT id FROM categories WHERE name = 'Internal Movement')
  AND t.goal_id IS NULL
  AND (
    a.type IN ('cash', 'bank', 'ewallet', 'regular')
    OR a.type IS NULL
  )
  AND (
    t.notes IS NULL
    OR t.notes = ''
    OR t.notes ILIKE '%payment%'
    OR t.notes ILIKE '%internal movement%'
    OR t.notes ILIKE '%pindah saldo%'
  );
