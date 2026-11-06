-- System category keys (add-category-system-keys D1, D3, D4). Additive; no row is deleted or renamed.
-- Money rules used to find the investment, movement and balance-adjustment categories by display name, so a
-- rename moved figures. `system_key` is their stable identity; ordinary categories keep NULL.
-- The backfill reproduces today's predicate exactly: the same exact, case-sensitive names, every kind and
-- archived rows included, because today's checks ignore both. A row renamed before this migration gets no
-- key, which is already how today's code treats it.
-- An older release ignores the column. Rollback: redeploy the previous release and leave the column; rename
-- any renamed system category back to its seed name first, or its figures change under the old code.
ALTER TABLE categories ADD COLUMN IF NOT EXISTS system_key VARCHAR(32) NULL;

ALTER TABLE categories DROP CONSTRAINT IF EXISTS categories_system_key_check;
ALTER TABLE categories ADD CONSTRAINT categories_system_key_check
    CHECK (system_key IN ('investment', 'internal_movement', 'balance_adjustment'));

UPDATE categories SET system_key = 'investment'
WHERE name = 'Investasi' AND system_key IS NULL;

UPDATE categories SET system_key = 'internal_movement'
WHERE name = 'Internal Movement' AND system_key IS NULL;

UPDATE categories SET system_key = 'balance_adjustment'
WHERE name = 'Penyesuaian Saldo' AND system_key IS NULL;

-- UNIQUE (user_id, name, kind) makes a duplicate impossible, since each key comes from one name. Check anyway
-- and fail, rolling the whole migration back, instead of guessing which row keeps the key.
DO $$
DECLARE
    duplicate_owners INTEGER;
BEGIN
    SELECT COUNT(DISTINCT user_id) INTO duplicate_owners
    FROM (
        SELECT user_id FROM categories
        WHERE system_key IS NOT NULL
        GROUP BY user_id, system_key, kind
        HAVING COUNT(*) > 1
    ) duplicates;
    IF duplicate_owners > 0 THEN
        RAISE EXCEPTION 'category system keys: % owner(s) have more than one category per key and kind', duplicate_owners;
    END IF;
END;
$$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_category_user_system_key_kind
    ON categories (user_id, system_key, kind)
    WHERE system_key IS NOT NULL;
