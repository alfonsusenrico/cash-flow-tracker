-- Capture time understanding (understand-capture-time): how a typed capture's time was decided and the
-- owner's own words it was read from. Additive: both columns are NULL on every existing row and on rows
-- of every other source (a notification keeps its post time). Rollback: drop the constraints and columns;
-- older code never reads them.

ALTER TABLE transactions ADD COLUMN IF NOT EXISTS time_basis VARCHAR(12) NULL;
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS time_evidence VARCHAR(200) NULL;

ALTER TABLE transactions DROP CONSTRAINT IF EXISTS chk_transactions_time_basis;
ALTER TABLE transactions ADD CONSTRAINT chk_transactions_time_basis CHECK (
    time_basis IS NULL OR time_basis IN ('device_time', 'parsed', 'model', 'owner')
);

-- Evidence exists only for a time read from the text, by the parser or the model.
ALTER TABLE transactions DROP CONSTRAINT IF EXISTS chk_transactions_time_evidence;
ALTER TABLE transactions ADD CONSTRAINT chk_transactions_time_evidence CHECK (
    time_evidence IS NULL OR time_basis IN ('parsed', 'model')
);
