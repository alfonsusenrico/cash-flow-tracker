-- Origin of a top-up converted from a recorded expense: restored when the top-up is deleted.
ALTER TABLE investment_topups ADD COLUMN IF NOT EXISTS converted_from JSONB;
