-- V34: Add initial_transaction_id to obligations
ALTER TABLE obligations ADD COLUMN initial_transaction_id UUID NULL REFERENCES transactions(transaction_id) ON DELETE SET NULL;
