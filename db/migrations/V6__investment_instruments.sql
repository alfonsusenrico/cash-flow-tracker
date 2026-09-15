-- V6__investment_instruments.sql
-- Add hybrid investment tracking columns to accounts

ALTER TABLE accounts ADD COLUMN IF NOT EXISTS instrument_type VARCHAR(30) NULL;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS instrument_symbol VARCHAR(30) NULL;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS units NUMERIC(18, 6) NULL;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS avg_buy_price BIGINT NULL;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS last_price BIGINT NULL;
ALTER TABLE accounts ADD COLUMN IF NOT EXISTS last_price_at TIMESTAMPTZ NULL;

CREATE INDEX IF NOT EXISTS idx_accounts_instrument_symbol ON accounts(instrument_symbol) WHERE instrument_symbol IS NOT NULL;
