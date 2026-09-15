-- init.sql: Personal Finance OS Relational Schema
-- 7-table architecture: users, accounts, categories, transactions, api_keys, goals, obligations

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- 1. Users
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(100) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    invite_code VARCHAR(100) NOT NULL,
    payday_day INT NOT NULL DEFAULT 25,
    currency VARCHAR(10) NOT NULL DEFAULT 'IDR',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Accounts (Physical holders of liquid cash: Cash, Bank, E-wallet)
CREATE TABLE IF NOT EXISTS accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    parent_id UUID NULL REFERENCES accounts(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    type VARCHAR(30) NOT NULL DEFAULT 'bank', -- 'cash', 'bank', 'wallet', 'investment'
    initial_balance BIGINT NOT NULL DEFAULT 0,
    instrument_type VARCHAR(30) NULL, -- 'stock', 'mutual_fund', 'gold', 'crypto', 'deposit', 'other'
    instrument_symbol VARCHAR(30) NULL,
    units NUMERIC(18, 6) NULL,
    avg_buy_price NUMERIC(18, 4) NULL,
    last_price NUMERIC(18, 4) NULL,
    last_price_at TIMESTAMPTZ NULL,
    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_account_user_name UNIQUE (user_id, name)
);
CREATE INDEX IF NOT EXISTS idx_accounts_user_id ON accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_accounts_parent ON accounts(parent_id);
CREATE INDEX IF NOT EXISTS idx_accounts_instrument_symbol ON accounts(instrument_symbol) WHERE instrument_symbol IS NOT NULL;

-- Schema evolution safe migrations
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS instrument_type VARCHAR(30) NULL;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS instrument_symbol VARCHAR(30) NULL;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS units NUMERIC(18, 6) NULL;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS avg_buy_price NUMERIC(18, 4) NULL;
ALTER TABLE accounts ALTER COLUMN avg_buy_price TYPE NUMERIC(18, 4);
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS last_price NUMERIC(18, 4) NULL;
ALTER TABLE accounts ALTER COLUMN last_price TYPE NUMERIC(18, 4);
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS last_price_at TIMESTAMPTZ NULL;

-- 3. Categories (Spending & income categories with optional monthly limits)
CREATE TABLE IF NOT EXISTS categories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    icon VARCHAR(50) NOT NULL DEFAULT 'tag',
    color VARCHAR(30) NOT NULL DEFAULT '#3b82f6',
    kind VARCHAR(20) NOT NULL DEFAULT 'expense', -- 'expense', 'income'
    monthly_budget BIGINT NULL,
    is_primary BOOLEAN NOT NULL DEFAULT TRUE,
    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_category_user_name_kind UNIQUE (user_id, name, kind)
);
CREATE INDEX IF NOT EXISTS idx_categories_user_id ON categories(user_id);

-- 4. Goals (Savings targets with pacing and deadlines)
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

-- 5. Obligations (Debts, loans, and recurring commitments)
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

-- 6. Transactions (Daily cashflow entries and atomic movements)
CREATE TABLE IF NOT EXISTS transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    category_id UUID NULL REFERENCES categories(id) ON DELETE SET NULL,
    goal_id UUID NULL REFERENCES goals(id) ON DELETE SET NULL,
    obligation_id UUID NULL REFERENCES obligations(id) ON DELETE SET NULL,
    type VARCHAR(20) NOT NULL, -- 'expense', 'income', 'transfer'
    transfer_target_account_id UUID NULL REFERENCES accounts(id) ON DELETE SET NULL,
    amount BIGINT NOT NULL,
    notes TEXT NULL,
    date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    receipt_path VARCHAR(255) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_tx_amount_positive CHECK (amount > 0)
);
CREATE INDEX IF NOT EXISTS idx_transactions_user_date ON transactions(user_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_account ON transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions(category_id);
CREATE INDEX IF NOT EXISTS idx_transactions_goal ON transactions(goal_id);
CREATE INDEX IF NOT EXISTS idx_transactions_obligation ON transactions(obligation_id);
CREATE INDEX IF NOT EXISTS idx_transactions_transfer_target ON transactions(transfer_target_account_id);

-- 7. API Keys (For external integrations such as Telegram Bot)
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
