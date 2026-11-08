-- Interface language and region per account (add-language-system D13).
-- Existing accounts were created and used in Indonesian, so they become 'id'/'ID';
-- accounts created after this migration default to English with region Indonesia.
ALTER TABLE users ADD COLUMN language VARCHAR(8) NOT NULL DEFAULT 'id';
ALTER TABLE users ALTER COLUMN language SET DEFAULT 'en';
ALTER TABLE users ADD CONSTRAINT ck_users_language CHECK (language IN ('en', 'id'));
-- Region sets number and date style only; users.currency keeps its display-currency meaning.
ALTER TABLE users ADD COLUMN region CHAR(2) NOT NULL DEFAULT 'ID';
ALTER TABLE users ADD CONSTRAINT ck_users_region CHECK (region IN ('ID', 'US'));
