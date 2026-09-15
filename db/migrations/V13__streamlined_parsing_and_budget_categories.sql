-- V13__streamlined_parsing_and_budget_categories.sql
-- Streamlined Parsing, Internal Movement & Investment Budget Rules

-- 1. Add is_excluded_from_budget flag to categories
ALTER TABLE categories ADD COLUMN IF NOT EXISTS is_excluded_from_budget BOOLEAN NOT NULL DEFAULT FALSE;

-- 2. Seed 'Internal Movement' category for all existing users
INSERT INTO categories (user_id, name, icon, color, kind, kakeibo_type, is_excluded_from_budget)
SELECT id, 'Internal Movement', 'arrows-right-left', '#3b82f6', 'expense', 'saving', TRUE
FROM users
ON CONFLICT (user_id, name, kind) DO UPDATE SET
  is_excluded_from_budget = TRUE;

-- 3. Seed 'Investasi' category for all existing users
INSERT INTO categories (user_id, name, icon, color, kind, kakeibo_type, is_excluded_from_budget)
SELECT id, 'Investasi', 'trending-up', '#10b981', 'expense', 'saving', TRUE
FROM users
ON CONFLICT (user_id, name, kind) DO UPDATE SET
  kakeibo_type = 'saving',
  is_excluded_from_budget = TRUE;

-- 4. Create merchant category rules table for learned categorizations
CREATE TABLE IF NOT EXISTS merchant_category_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    merchant_pattern VARCHAR(255) NOT NULL,
    category_id UUID NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_user_merchant_rule UNIQUE (user_id, merchant_pattern)
);
CREATE INDEX IF NOT EXISTS idx_merchant_rules_user ON merchant_category_rules(user_id);

-- 5. Extend notification_events to link with transactions and hold parsed summaries
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS parsed_summary JSONB NULL;
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS transaction_id UUID NULL REFERENCES transactions(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_notifications_transaction_id ON notification_events(transaction_id) WHERE transaction_id IS NOT NULL;
