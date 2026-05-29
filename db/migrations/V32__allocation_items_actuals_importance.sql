-- MVP-1: category_id for plan-vs-actual reporting
-- MVP-2: importance classification (mandatory / standard / flexible)
-- MVP-18: notes per item

ALTER TABLE allocation_items
  ADD COLUMN IF NOT EXISTS category_id UUID NULL REFERENCES categories(category_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS importance TEXT NOT NULL DEFAULT 'standard'
    CHECK (importance IN ('mandatory','standard','flexible')),
  ADD COLUMN IF NOT EXISTS notes TEXT NULL;

CREATE INDEX IF NOT EXISTS idx_allocation_items_category
  ON allocation_items(category_id) WHERE category_id IS NOT NULL;
