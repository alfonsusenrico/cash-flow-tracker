-- Tags the owner puts on transactions to group rows across categories (transaction-tags D1).
-- `name` keeps the owner's spelling for display; `name_normalized` (lower-cased, inner whitespace collapsed to one
-- space, trimmed) is computed by the API and makes "Fellowship  Coach" the same tag as "fellowship coach".
CREATE TABLE IF NOT EXISTS tags (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (char_length(btrim(name)) BETWEEN 1 AND 40),
    name_normalized TEXT NOT NULL CHECK (char_length(name_normalized) BETWEEN 1 AND 40),
    archived_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, name_normalized)
);

-- A tag on a transaction. Deleting either side deletes the link; movements carry no tags.
CREATE TABLE IF NOT EXISTS transaction_tags (
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    tag_id UUID NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_id, tag_id)
);

-- The ledger tag filter and the per-tag row counts look links up by tag.
CREATE INDEX IF NOT EXISTS idx_transaction_tags_user_tag ON transaction_tags (user_id, tag_id);
