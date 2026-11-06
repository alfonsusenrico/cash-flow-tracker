-- Credit purchase terms, the bill day and the credit-fee category key (add-credit-spending-flow D1, D2, D8, D15).
-- Additive and safe to run twice. An older release ignores the column and the table, and `credit_fee` is
-- outside its non-reportable keys, so fees still count as spending there. Rollback: redeploy the previous
-- release and leave the column, the table and the key unused.

-- The day a credit account's bill is issued; credit-only like payment_day, which the API enforces.
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS statement_day SMALLINT NULL;
ALTER TABLE accounts DROP CONSTRAINT IF EXISTS accounts_statement_day_check;
ALTER TABLE accounts ADD CONSTRAINT accounts_statement_day_check
    CHECK (statement_day IS NULL OR statement_day BETWEEN 1 AND 31);

-- No row means paid in full with a derived due date, so existing purchases need no backfill. Only an
-- expense on a credit account may have a row; the API enforces it and removes the row when that stops
-- being true. A first due date always records who set it.
CREATE TABLE IF NOT EXISTS credit_purchase_terms (
    transaction_id UUID PRIMARY KEY REFERENCES transactions(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    installment_count SMALLINT NOT NULL DEFAULT 1,
    first_due_date DATE NULL,
    first_due_date_set_by VARCHAR(8) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT credit_purchase_terms_installment_count_check CHECK (installment_count BETWEEN 1 AND 36),
    CONSTRAINT credit_purchase_terms_set_by_check CHECK (first_due_date_set_by IN ('owner', 'system')),
    CONSTRAINT credit_purchase_terms_due_setter_check CHECK ((first_due_date IS NULL) = (first_due_date_set_by IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_credit_purchase_terms_user ON credit_purchase_terms (user_id);

ALTER TABLE categories DROP CONSTRAINT IF EXISTS categories_system_key_check;
ALTER TABLE categories ADD CONSTRAINT categories_system_key_check
    CHECK (system_key IN ('investment', 'internal_movement', 'balance_adjustment', 'credit_fee'));

-- The fee category was found by its exact name and kind; only that row gets the key, and only while the
-- owner has no keyed fee category yet. A renamed fee category keeps no key, as today's code ignores it.
UPDATE categories c SET system_key = 'credit_fee'
WHERE c.name = 'Biaya kredit' AND c.kind = 'expense' AND c.system_key IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM categories keyed
      WHERE keyed.user_id = c.user_id AND keyed.system_key = 'credit_fee' AND keyed.kind = 'expense'
  );

-- UNIQUE (user_id, name, kind) makes a duplicate impossible. Check anyway and fail, rolling the whole
-- migration back, instead of guessing which row keeps the key.
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
