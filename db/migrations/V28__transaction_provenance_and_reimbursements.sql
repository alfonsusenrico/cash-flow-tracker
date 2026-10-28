-- Transaction provenance, reimbursement offsets and the snack/café category.
-- Idempotent: the API runs an identical copy at startup
-- (backend/app/db/transaction_provenance_and_reimbursements.sql). Everything that writes data is
-- guarded by the absence of transactions.source, so the backfill runs exactly once and a later
-- restart never overwrites review decisions or recreates a category the owner deleted.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_attribute
        WHERE attrelid = 'transactions'::regclass AND attname = 'source' AND NOT attisdropped
    ) THEN
        -- Defaults exist only so that a rolled-back release, which does not name these columns,
        -- can still insert; the application always writes them explicitly.
        ALTER TABLE transactions
            ADD COLUMN source VARCHAR(20) NOT NULL DEFAULT 'manual',
            ADD COLUMN merchant VARCHAR(120) NULL,
            ADD COLUMN review_status VARCHAR(20) NOT NULL DEFAULT 'reviewed',
            ADD COLUMN classified_by VARCHAR(10) NULL,
            ADD COLUMN classification_confidence NUMERIC(4, 3) NULL,
            ADD COLUMN owner_edited_at TIMESTAMPTZ NULL,
            ADD COLUMN offset_category_id UUID NULL REFERENCES categories(id) ON DELETE SET NULL,
            ADD CONSTRAINT chk_transactions_source CHECK (
                source IN ('manual', 'notification', 'recurring', 'payroll', 'movement',
                           'reconcile', 'investment', 'import')
            ),
            ADD CONSTRAINT chk_transactions_merchant CHECK (
                merchant IS NULL OR char_length(btrim(merchant)) BETWEEN 1 AND 120
            ),
            ADD CONSTRAINT chk_transactions_review_status CHECK (
                review_status IN ('auto', 'needs_review', 'reviewed')
            ),
            ADD CONSTRAINT chk_transactions_classified_by CHECK (
                classified_by IS NULL OR classified_by IN ('rule', 'ai', 'owner', 'system')
            ),
            ADD CONSTRAINT chk_transactions_classification_confidence CHECK (
                classification_confidence IS NULL
                OR (classification_confidence >= 0 AND classification_confidence <= 1)
            ),
            ADD CONSTRAINT chk_transactions_offset_income CHECK (
                offset_category_id IS NULL OR type = 'income'
            );

        ALTER TABLE categories
            ADD COLUMN offsets_spending BOOLEAN NOT NULL DEFAULT FALSE,
            ADD CONSTRAINT chk_categories_offsets_spending_income CHECK (
                NOT offsets_spending OR kind = 'income'
            );

        -- Source: the first matching rule wins (design D2). Only the new columns are written, so
        -- updated_at and every money field keep their values.
        UPDATE transactions t
        SET source = CASE
            WHEN t.idempotency_key LIKE 'notification:%'
                 OR EXISTS (
                     SELECT 1 FROM notification_events n
                     WHERE n.transaction_id = t.id AND n.user_id = t.user_id
                 ) THEN 'notification'
            WHEN t.idempotency_key LIKE 'recurring:%' THEN 'recurring'
            WHEN t.idempotency_key LIKE 'payroll:%' THEN 'payroll'
            WHEN t.recurring_rule_id IS NOT NULL THEN 'recurring'
            WHEN t.movement_id IS NOT NULL AND (
                EXISTS (
                    SELECT 1 FROM investment_topups topup
                    WHERE topup.id = t.movement_id AND topup.user_id = t.user_id
                )
                OR EXISTS (
                    SELECT 1 FROM transactions leg
                    JOIN accounts a ON a.id = leg.account_id
                    WHERE leg.movement_id = t.movement_id AND leg.user_id = t.user_id
                      AND (a.type = 'investment' OR a.instrument_type IS NOT NULL)
                )
            ) THEN 'investment'
            WHEN EXISTS (
                SELECT 1 FROM categories c
                WHERE c.id = t.category_id AND c.user_id = t.user_id AND c.name = 'Penyesuaian Saldo'
            ) THEN 'reconcile'
            WHEN t.movement_id IS NOT NULL THEN 'movement'
            ELSE 'manual'
        END;

        -- Classifier of notification rows: the event named by the idempotency key, else the earliest
        -- event linked to the row.
        UPDATE transactions t
        SET classified_by = CASE
            WHEN t.source = 'manual' THEN 'owner'
            WHEN t.source <> 'notification' THEN 'system'
            ELSE COALESCE((
                SELECT CASE n.interpretation ->> 'source'
                           WHEN 'ai' THEN 'ai'
                           WHEN 'manual' THEN 'owner'
                           ELSE 'rule'
                       END
                FROM notification_events n
                WHERE n.user_id = t.user_id
                  AND (
                      n.id::text = split_part(t.idempotency_key, ':', 2)
                      OR n.transaction_id = t.id
                  )
                ORDER BY (n.id::text = split_part(COALESCE(t.idempotency_key, ''), ':', 2)) DESC,
                         n.created_at, n.id
                LIMIT 1
            ), 'rule')
        END;

        UPDATE transactions t
        SET review_status = CASE
            WHEN t.source = 'notification'
                 AND t.classified_by <> 'owner'
                 AND t.updated_at <= t.created_at + INTERVAL '1 minute' THEN 'auto'
            ELSE 'reviewed'
        END;

        -- A friend repaying a shared expense offsets that spending; it is not income.
        INSERT INTO categories (user_id, name, icon, color, kind, kakeibo_type, offsets_spending)
        SELECT u.id, 'Patungan & Ganti Uang', 'users', '#0d9488', 'income', NULL, TRUE
        FROM users u
        ON CONFLICT (user_id, name, kind) DO UPDATE
        SET offsets_spending = TRUE, is_excluded_from_budget = FALSE, is_archived = FALSE;

        -- Snacks and cafés are discretionary, unlike daily meals. An existing category keeps its
        -- settings, and no transaction is moved into it.
        INSERT INTO categories (user_id, name, icon, color, kind, kakeibo_type)
        SELECT u.id, 'Jajan & Nongkrong', 'coffee', '#d97706', 'expense', 'want'
        FROM users u
        ON CONFLICT (user_id, name, kind) DO NOTHING;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_transactions_review_queue
    ON transactions(user_id, review_status, date DESC);
