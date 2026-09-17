-- V14__remove_transfer_type_and_add_default_pocket.sql
-- Remove 'transfer' transaction type, migrate existing transfers to paired expense/income,
-- add default_pocket_id to accounts.

-- 1. Seed 'Internal Movement' categories (expense and income kind) for all users
INSERT INTO categories (user_id, name, icon, color, kind, kakeibo_type, is_excluded_from_budget)
SELECT id, 'Internal Movement', 'arrows-right-left', '#3b82f6', 'expense', 'saving', TRUE
FROM users
ON CONFLICT (user_id, name, kind) DO UPDATE SET
  is_excluded_from_budget = TRUE;

INSERT INTO categories (user_id, name, icon, color, kind, kakeibo_type, is_excluded_from_budget)
SELECT id, 'Internal Movement', 'arrows-right-left', '#3b82f6', 'income', NULL, TRUE
FROM users
ON CONFLICT (user_id, name, kind) DO UPDATE SET
  is_excluded_from_budget = TRUE;

-- 2. For any existing transfer transactions (safely handle if transfer_target_account_id exists):
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'transactions' 
          AND column_name = 'transfer_target_account_id'
    ) THEN
        EXECUTE '
            INSERT INTO transactions (
                user_id,
                account_id,
                category_id,
                type,
                amount,
                notes,
                date,
                receipt_path,
                kakeibo_type,
                idempotency_key
            )
            SELECT 
                t.user_id,
                t.transfer_target_account_id,
                c.id,
                ''income'',
                t.amount,
                t.notes,
                t.date,
                t.receipt_path,
                NULL,
                ''migrated-in-'' || t.id::text
            FROM transactions t
            JOIN categories c ON c.user_id = t.user_id AND c.name = ''Internal Movement'' AND c.kind = ''income''
            WHERE t.type = ''transfer''
              AND t.transfer_target_account_id IS NOT NULL;

            UPDATE transactions t
            SET type = ''expense'',
                category_id = c.id,
                kakeibo_type = ''saving'',
                idempotency_key = COALESCE(t.idempotency_key, ''migrated-out-'' || t.id::text)
            FROM categories c
            WHERE t.type = ''transfer''
              AND c.user_id = t.user_id
              AND c.name = ''Internal Movement''
              AND c.kind = ''expense'';
        ';
    END IF;
END $$;

-- 3. Drop transfer_target_account_id constraint, index, and column
DROP INDEX IF EXISTS idx_transactions_transfer_target;
ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_transfer_target_account_id_fkey;
ALTER TABLE transactions DROP COLUMN IF EXISTS transfer_target_account_id;

-- 4. Enforce transactions.type constraint (expense, income only)
ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_type_check;
ALTER TABLE transactions ADD CONSTRAINT transactions_type_check CHECK (type IN ('expense', 'income'));

-- 5. Add default_pocket_id to accounts
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS default_pocket_id UUID REFERENCES accounts(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_accounts_default_pocket ON accounts(default_pocket_id);
