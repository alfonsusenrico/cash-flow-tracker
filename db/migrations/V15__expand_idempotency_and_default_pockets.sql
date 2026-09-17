-- V15__expand_idempotency_and_default_pockets.sql
-- Ensure idempotency_key VARCHAR(128) and assign default_pocket_id for parent accounts

ALTER TABLE transactions ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(128);
ALTER TABLE transactions ALTER COLUMN idempotency_key TYPE VARCHAR(128);

-- 1. Assign default_pocket_id prioritizing common main/ATM/checking pocket names
UPDATE accounts p
SET default_pocket_id = c.id
FROM (
    SELECT DISTINCT ON (parent_id) id, parent_id
    FROM accounts
    WHERE parent_id IS NOT NULL AND is_archived = FALSE
      AND (
        LOWER(name) IN ('atm', 'kantong utama', 'main pocket', 'utama', 'tabungan', 'checking')
        OR LOWER(name) LIKE '%atm%'
        OR LOWER(name) LIKE '%utama%'
        OR LOWER(name) LIKE '%tabungan%'
      )
    ORDER BY parent_id,
      CASE
        WHEN LOWER(name) = 'atm' THEN 1
        WHEN LOWER(name) IN ('kantong utama', 'main pocket', 'utama') THEN 2
        WHEN LOWER(name) = 'tabungan' THEN 3
        ELSE 4
      END,
      display_order ASC, created_at ASC
) c
WHERE p.id = c.parent_id AND p.default_pocket_id IS NULL;

-- 2. Fallback to first active child pocket for any remaining parent accounts with children
UPDATE accounts p
SET default_pocket_id = c.id
FROM (
    SELECT DISTINCT ON (parent_id) id, parent_id
    FROM accounts
    WHERE parent_id IS NOT NULL AND is_archived = FALSE
    ORDER BY parent_id, display_order ASC, created_at ASC
) c
WHERE p.id = c.parent_id AND p.default_pocket_id IS NULL;

-- 3. Re-route any transactions mistakenly recorded directly on parent accounts to their default pocket
UPDATE transactions t
SET account_id = p.default_pocket_id
FROM accounts p
WHERE t.account_id = p.id
  AND p.default_pocket_id IS NOT NULL;
