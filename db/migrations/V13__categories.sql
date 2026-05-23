-- Phase 1: Categories table.
-- Categories classify what a transaction is FOR, independent of which account holds the money.
-- kind: 'income' | 'expense' | 'transfer' | 'adjustment'

CREATE TABLE IF NOT EXISTS categories (
  category_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id       UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  name          TEXT NOT NULL,
  kind          TEXT NOT NULL DEFAULT 'expense'
                  CHECK (kind IN ('income', 'expense', 'transfer', 'adjustment')),
  parent_category_id UUID NULL REFERENCES categories(category_id) ON DELETE SET NULL,
  color         TEXT NULL,       -- optional hex color for UI
  icon          TEXT NULL,       -- optional icon slug
  is_archived   BOOLEAN NOT NULL DEFAULT FALSE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (user_id, name)
);

CREATE INDEX IF NOT EXISTS idx_categories_user_id ON categories(user_id);
CREATE INDEX IF NOT EXISTS idx_categories_user_kind ON categories(user_id, kind) WHERE is_archived = FALSE;

-- Seed default categories for every existing user.
-- Users can rename/archive these; they are just starting points.
INSERT INTO categories (user_id, name, kind)
SELECT u.user_id, c.name, c.kind
FROM users u
CROSS JOIN (VALUES
  ('Salary',        'income'),
  ('Freelance',     'income'),
  ('Other Income',  'income'),
  ('Food & Drink',  'expense'),
  ('Transport',     'expense'),
  ('Shopping',      'expense'),
  ('Health',        'expense'),
  ('Entertainment', 'expense'),
  ('Bills',         'expense'),
  ('Education',     'expense'),
  ('Other Expense', 'expense'),
  ('Transfer',      'transfer')
) AS c(name, kind)
ON CONFLICT (user_id, name) DO NOTHING;
