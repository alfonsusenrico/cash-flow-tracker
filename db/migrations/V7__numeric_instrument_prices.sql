-- V7__numeric_instrument_prices.sql
-- Allow decimal/fractional prices for mutual funds (NAB), gold, and crypto

ALTER TABLE accounts ALTER COLUMN avg_buy_price TYPE NUMERIC(18, 4);
ALTER TABLE accounts ALTER COLUMN last_price TYPE NUMERIC(18, 4);
