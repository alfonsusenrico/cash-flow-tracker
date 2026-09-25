import uuid
from typing import Any
from app.db.pool import db_conn

DEFAULT_CATEGORIES = [
    {"name": "Makanan & Minuman", "icon": "utensils", "color": "#f97316", "kind": "expense"},
    {"name": "Belanja Harian", "icon": "shopping-cart", "color": "#10b981", "kind": "expense"},
    {"name": "Transportasi", "icon": "car", "color": "#3b82f6", "kind": "expense"},
    {"name": "Tagihan & Utilitas", "icon": "zap", "color": "#eab308", "kind": "expense"},
    {"name": "Belanja", "icon": "shopping-bag", "color": "#ec4899", "kind": "expense"},
    {"name": "Hiburan", "icon": "film", "color": "#8b5cf6", "kind": "expense"},
    {"name": "Kesehatan", "icon": "heart", "color": "#ef4444", "kind": "expense"},
    {"name": "Investasi", "icon": "trending-up", "color": "#0ea5e9", "kind": "expense"},
    {"name": "Internal Movement", "icon": "repeat", "color": "#64748b", "kind": "expense"},
    {"name": "Internal Movement", "icon": "repeat", "color": "#64748b", "kind": "income"},
    {"name": "Gaji", "icon": "dollar-sign", "color": "#22c55e", "kind": "income"},
    {"name": "Pendapatan Lain", "icon": "plus-circle", "color": "#14b8a6", "kind": "income"},
]

DEFAULT_ACCOUNTS = [
    {"name": "Uang Tunai", "type": "cash", "initial_balance": 0},
    {"name": "Rekening Utama", "type": "bank", "initial_balance": 0},
]


def init_db_schema() -> None:
    """Ensure 7-table personal finance OS schema exists."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE EXTENSION IF NOT EXISTS "pgcrypto";

                CREATE TABLE IF NOT EXISTS users (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    username VARCHAR(100) UNIQUE NOT NULL,
                    name VARCHAR(150) NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    invite_code VARCHAR(100) NOT NULL,
                    payday_day INT NOT NULL DEFAULT 25,
                    currency VARCHAR(10) NOT NULL DEFAULT 'IDR',
                    emergency_fund_multiplier INT NOT NULL DEFAULT 6,
                    monthly_spending_budget BIGINT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS accounts (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    parent_id UUID NULL REFERENCES accounts(id) ON DELETE CASCADE,
                    default_funding_account_id UUID NULL REFERENCES accounts(id) ON DELETE SET NULL,
                    default_pocket_id UUID NULL REFERENCES accounts(id) ON DELETE SET NULL,
                    name VARCHAR(100) NOT NULL,
                    type VARCHAR(30) NOT NULL DEFAULT 'bank',
                    initial_balance BIGINT NOT NULL DEFAULT 0,
                    instrument_type VARCHAR(30) NULL,
                    instrument_symbol VARCHAR(30) NULL,
                    units NUMERIC(18, 6) NULL,
                    avg_buy_price BIGINT NULL,
                    last_price BIGINT NULL,
                    last_price_at TIMESTAMPTZ NULL,
                    color VARCHAR(30) NOT NULL DEFAULT '#3b82f6',
                    display_order INT NOT NULL DEFAULT 0,
                    reconciliation_required BOOLEAN NOT NULL DEFAULT FALSE,
                    reconciliation_reason VARCHAR(100) NULL,
                    reconciliation_event_id UUID NULL,
                    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT uq_account_user_name UNIQUE (user_id, name)
                );
                CREATE INDEX IF NOT EXISTS idx_accounts_user_id ON accounts(user_id);
                CREATE INDEX IF NOT EXISTS idx_accounts_parent ON accounts(parent_id);
                CREATE INDEX IF NOT EXISTS idx_accounts_default_pocket ON accounts(default_pocket_id);
                CREATE INDEX IF NOT EXISTS idx_accounts_display_order ON accounts(user_id, display_order);
                CREATE INDEX IF NOT EXISTS idx_accounts_instrument_symbol ON accounts(instrument_symbol) WHERE instrument_symbol IS NOT NULL;

                CREATE TABLE IF NOT EXISTS categories (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    name VARCHAR(100) NOT NULL,
                    icon VARCHAR(50) NOT NULL DEFAULT 'tag',
                    color VARCHAR(30) NOT NULL DEFAULT '#3b82f6',
                    kind VARCHAR(20) NOT NULL DEFAULT 'expense',
                    monthly_budget BIGINT NULL,
                    is_primary BOOLEAN NOT NULL DEFAULT TRUE,
                    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT uq_category_user_name_kind UNIQUE (user_id, name, kind)
                );
                CREATE INDEX IF NOT EXISTS idx_categories_user_id ON categories(user_id);

                CREATE TABLE IF NOT EXISTS goals (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    name VARCHAR(100) NOT NULL,
                    target_amount BIGINT NOT NULL,
                    current_amount BIGINT NOT NULL DEFAULT 0,
                    target_date DATE NULL,
                    color VARCHAR(30) NOT NULL DEFAULT '#10b981',
                    icon VARCHAR(50) NOT NULL DEFAULT 'target',
                    is_emergency BOOLEAN NOT NULL DEFAULT FALSE,
                    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT uq_goal_user_name UNIQUE (user_id, name),
                    CONSTRAINT chk_goal_target_positive CHECK (target_amount > 0),
                    CONSTRAINT chk_goal_current_non_negative CHECK (current_amount >= 0)
                );
                CREATE INDEX IF NOT EXISTS idx_goals_user_id ON goals(user_id);
                CREATE INDEX IF NOT EXISTS idx_goals_is_emergency ON goals(user_id, is_emergency);

                CREATE TABLE IF NOT EXISTS goal_accounts (
                    goal_id UUID NOT NULL REFERENCES goals(id) ON DELETE CASCADE,
                    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (goal_id, account_id)
                );
                CREATE INDEX IF NOT EXISTS idx_goal_accounts_goal ON goal_accounts(goal_id);
                CREATE INDEX IF NOT EXISTS idx_goal_accounts_account ON goal_accounts(account_id);

                CREATE TABLE IF NOT EXISTS obligations (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    name VARCHAR(100) NOT NULL,
                    total_amount BIGINT NOT NULL,
                    remaining_amount BIGINT NOT NULL,
                    due_date DATE NULL,
                    minimum_payment BIGINT NULL,
                    notes TEXT NULL,
                    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT uq_obligation_user_name UNIQUE (user_id, name),
                    CONSTRAINT chk_obligation_total_positive CHECK (total_amount > 0),
                    CONSTRAINT chk_obligation_remaining_non_negative CHECK (remaining_amount >= 0)
                );
                CREATE INDEX IF NOT EXISTS idx_obligations_user_id ON obligations(user_id);

                CREATE TABLE IF NOT EXISTS transactions (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                    category_id UUID NULL REFERENCES categories(id) ON DELETE SET NULL,
                    goal_id UUID NULL REFERENCES goals(id) ON DELETE SET NULL,
                    obligation_id UUID NULL REFERENCES obligations(id) ON DELETE SET NULL,
                    type VARCHAR(20) NOT NULL,
                    amount BIGINT NOT NULL,
                    notes TEXT NULL,
                    date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    receipt_path VARCHAR(255) NULL,
                    movement_id UUID NULL,
                    movement_role VARCHAR(10) NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT chk_tx_amount_positive CHECK (amount > 0)
                );
                CREATE INDEX IF NOT EXISTS idx_transactions_user_date ON transactions(user_id, date DESC);
                CREATE INDEX IF NOT EXISTS idx_transactions_account ON transactions(account_id);
                CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions(category_id);
                CREATE INDEX IF NOT EXISTS idx_transactions_goal ON transactions(goal_id);
                CREATE INDEX IF NOT EXISTS idx_transactions_obligation ON transactions(obligation_id);

                CREATE TABLE IF NOT EXISTS transaction_obligation_allocations (
                    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
                    obligation_id UUID NOT NULL REFERENCES obligations(id) ON DELETE RESTRICT,
                    amount BIGINT NOT NULL CHECK (amount > 0),
                    PRIMARY KEY (transaction_id, obligation_id)
                );
                CREATE INDEX IF NOT EXISTS idx_transaction_obligation_allocations_obligation
                    ON transaction_obligation_allocations(obligation_id);

                CREATE TABLE IF NOT EXISTS api_keys (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    key_hash VARCHAR(64) NOT NULL UNIQUE,
                    key_prefix VARCHAR(10) NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_used_at TIMESTAMPTZ NULL
                );
                CREATE INDEX IF NOT EXISTS idx_api_keys_user_id ON api_keys(user_id);
                CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);

                -- Safe migrations for evolving existing schemas
                ALTER TABLE users ADD COLUMN IF NOT EXISTS name VARCHAR(150) NULL;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS emergency_fund_multiplier INT NOT NULL DEFAULT 6;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS monthly_spending_budget BIGINT NULL;
                DELETE FROM transactions WHERE notes LIKE 'Penyesuaian Nilai Investasi%';

                ALTER TABLE accounts ADD COLUMN IF NOT EXISTS instrument_type VARCHAR(30) NULL;
                ALTER TABLE accounts ADD COLUMN IF NOT EXISTS instrument_symbol VARCHAR(30) NULL;
                ALTER TABLE accounts ADD COLUMN IF NOT EXISTS units NUMERIC(18, 6) NULL;
                ALTER TABLE accounts ADD COLUMN IF NOT EXISTS avg_buy_price BIGINT NULL;
                ALTER TABLE accounts ADD COLUMN IF NOT EXISTS last_price BIGINT NULL;
                ALTER TABLE accounts ADD COLUMN IF NOT EXISTS last_price_at TIMESTAMPTZ NULL;
                ALTER TABLE accounts ADD COLUMN IF NOT EXISTS color VARCHAR(30) NOT NULL DEFAULT '#3b82f6';
                ALTER TABLE accounts ADD COLUMN IF NOT EXISTS display_order INT NOT NULL DEFAULT 0;
                ALTER TABLE accounts ADD COLUMN IF NOT EXISTS default_funding_account_id UUID NULL REFERENCES accounts(id) ON DELETE SET NULL;
                ALTER TABLE accounts ADD COLUMN IF NOT EXISTS default_pocket_id UUID NULL REFERENCES accounts(id) ON DELETE SET NULL;
                ALTER TABLE accounts ADD COLUMN IF NOT EXISTS reconciliation_required BOOLEAN NOT NULL DEFAULT FALSE;
                ALTER TABLE accounts ADD COLUMN IF NOT EXISTS reconciliation_reason VARCHAR(100) NULL;
                ALTER TABLE accounts ADD COLUMN IF NOT EXISTS reconciliation_event_id UUID NULL;
                CREATE INDEX IF NOT EXISTS idx_accounts_display_order ON accounts(user_id, display_order);
                CREATE INDEX IF NOT EXISTS idx_accounts_instrument_symbol ON accounts(instrument_symbol) WHERE instrument_symbol IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_accounts_default_pocket ON accounts(default_pocket_id);

                ALTER TABLE transactions ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(128) NULL;
                ALTER TABLE transactions ALTER COLUMN idempotency_key TYPE VARCHAR(128);
                ALTER TABLE transactions ADD COLUMN IF NOT EXISTS movement_id UUID NULL;
                ALTER TABLE transactions ADD COLUMN IF NOT EXISTS movement_role VARCHAR(10) NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS uq_tx_user_idempotency ON transactions(user_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS uq_transactions_movement_role ON transactions(movement_id, movement_role) WHERE movement_id IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_transactions_movement_id ON transactions(movement_id) WHERE movement_id IS NOT NULL;
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'chk_transactions_movement_role'
                    ) THEN
                        ALTER TABLE transactions
                        ADD CONSTRAINT chk_transactions_movement_role
                        CHECK (
                            (movement_id IS NULL AND movement_role IS NULL)
                            OR (
                                movement_id IS NOT NULL
                                AND movement_role IN ('outbound', 'inbound')
                            )
                        );
                    END IF;
                END $$;
                ALTER TABLE transactions DROP COLUMN IF EXISTS transfer_target_account_id;

                DO $$
                BEGIN
                    IF to_regclass('public.recurring_rules') IS NOT NULL THEN
                        CREATE TABLE IF NOT EXISTS recurring_executions (
                            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            recurring_rule_id UUID NOT NULL REFERENCES recurring_rules(id) ON DELETE CASCADE,
                            user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                            scheduled_for DATE NOT NULL,
                            status VARCHAR(20) NOT NULL,
                            error_code VARCHAR(100) NULL,
                            error_detail TEXT NULL,
                            transaction_id UUID NULL REFERENCES transactions(id) ON DELETE SET NULL,
                            movement_id UUID NULL,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            CONSTRAINT uq_recurring_execution_occurrence
                                UNIQUE (recurring_rule_id, scheduled_for),
                            CONSTRAINT chk_recurring_execution_status
                                CHECK (status IN ('processing', 'succeeded', 'failed'))
                        );
                        CREATE INDEX IF NOT EXISTS idx_recurring_executions_user_status
                            ON recurring_executions(user_id, status, scheduled_for DESC);
                    END IF;
                END $$;

                -- Auto-archive any existing paid-off obligations
                UPDATE obligations
                SET is_archived = true, updated_at = NOW()
                WHERE remaining_amount <= 0 AND is_archived = false;
            """)
            conn.commit()


def seed_user_defaults(user_id: str | uuid.UUID) -> None:
    """Seeds default categories and starter accounts for a new user if empty."""
    uid = str(user_id)
    with db_conn() as conn:
        with conn.cursor() as cur:
            # Check existing categories
            cur.execute("SELECT COUNT(*) AS c FROM categories WHERE user_id = %s", (uid,))
            row = cur.fetchone()
            if row and row["c"] == 0:
                for cat in DEFAULT_CATEGORIES:
                    cur.execute("""
                        INSERT INTO categories (user_id, name, icon, color, kind)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (uid, cat["name"], cat["icon"], cat["color"], cat["kind"]))

            # Check existing accounts
            cur.execute("SELECT COUNT(*) AS c FROM accounts WHERE user_id = %s", (uid,))
            row = cur.fetchone()
            if row and row["c"] == 0:
                for acc in DEFAULT_ACCOUNTS:
                    cur.execute("""
                        INSERT INTO accounts (user_id, name, type, initial_balance)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (uid, acc["name"], acc["type"], acc["initial_balance"]))
            conn.commit()
