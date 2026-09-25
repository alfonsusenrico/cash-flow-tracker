CREATE TABLE IF NOT EXISTS transaction_obligation_allocations (
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    obligation_id UUID NOT NULL REFERENCES obligations(id) ON DELETE RESTRICT,
    amount BIGINT NOT NULL CHECK (amount > 0),
    PRIMARY KEY (transaction_id, obligation_id)
);

CREATE INDEX IF NOT EXISTS idx_transaction_obligation_allocations_obligation
    ON transaction_obligation_allocations (obligation_id);
