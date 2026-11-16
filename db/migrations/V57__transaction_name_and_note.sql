-- A transaction's text was always its name; it gets that name, and a separate note becomes the owner's own
-- remark (separate-transaction-name-and-note D1). A rename is a catalogue change: no row is rewritten.
ALTER TABLE transactions RENAME COLUMN notes TO name;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS note TEXT;
