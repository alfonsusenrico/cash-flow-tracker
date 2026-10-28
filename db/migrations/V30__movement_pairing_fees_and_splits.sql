-- Resilient movement pairing: owner split marker and transfer-fee rows traced to their movement.
-- Idempotent: the identical startup mirror runs after Flyway on every API start.

-- Set when the owner splits a movement; automatic pairing never links such a row again.
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS owner_split_at TIMESTAMPTZ NULL;

-- The movement whose outbound debit included this fee expense. Not a foreign key: a movement id
-- is shared by its two legs and has no row of its own.
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS fee_for_movement_id UUID NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_transactions_fee_for_movement
    ON transactions(user_id, fee_for_movement_id) WHERE fee_for_movement_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_transactions_unpaired_by_date
    ON transactions(user_id, date) WHERE movement_id IS NULL AND owner_split_at IS NULL;
