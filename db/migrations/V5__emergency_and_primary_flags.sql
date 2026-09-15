-- V5__emergency_and_primary_flags.sql
-- Add is_emergency to goals and is_primary to categories for Ketahanan Dana calculation

-- 1. Goals: Add is_emergency flag
ALTER TABLE goals ADD COLUMN IF NOT EXISTS is_emergency BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX IF NOT EXISTS idx_goals_is_emergency ON goals(user_id, is_emergency);

-- Auto-flag existing goals matching "darurat" or "emergency"
UPDATE goals
SET is_emergency = TRUE
WHERE name ILIKE '%darurat%' OR name ILIKE '%emergency%';

-- 2. Categories: Add is_primary flag for essential vs discretionary living expense tracking
ALTER TABLE categories ADD COLUMN IF NOT EXISTS is_primary BOOLEAN NOT NULL DEFAULT TRUE;

-- Classify common discretionary categories as non-primary
UPDATE categories
SET is_primary = FALSE
WHERE kind = 'expense'
  AND (
    name ILIKE '%hiburan%'
    OR name ILIKE '%liburan%'
    OR name ILIKE '%belanja%'
    OR name ILIKE '%hobi%'
    OR name ILIKE '%jajan%'
    OR name ILIKE '%entertainment%'
    OR name ILIKE '%shopping%'
    OR name ILIKE '%lifestyle%'
  );
