-- Synthetic edge rows (add-category-system-keys D10), added to the parity seed at V51 before the database is
-- copied, so the "before" (V51 code) and "after" (V52 code) stacks read the same rows. All names and amounts are
-- synthetic. Dates are within the current salary cycle of the parity owner (dimas_contoh).
BEGIN;
WITH owner AS (SELECT id FROM users WHERE username = 'dimas_contoh'),
     utama AS (SELECT a.id FROM accounts a JOIN accounts p ON p.id = a.parent_id WHERE p.name = 'BCA' AND a.name = 'Utama'),
     new_categories AS (
         INSERT INTO categories (user_id, name, icon, color, kind, kakeibo_type, is_excluded_from_budget)
         SELECT owner.id, v.name, 'tag', '#64748b', v.kind, v.pillar, v.excluded
         FROM owner, (VALUES
             ('Investasi', 'expense', NULL, FALSE),        -- seeded name: investment buys are not spending
             ('Investasi', 'income', NULL, FALSE),         -- an income "Investasi": not income in reports
             ('investasi', 'expense', 'want', FALSE),      -- lower case: today it counts as spending
             ('Invest', 'expense', 'want', FALSE),         -- renamed before the migration: already spending
             ('Penyesuaian Saldo', 'expense', NULL, TRUE)  -- adjustment: excluded by its flag
         ) AS v(name, kind, pillar, excluded)
         RETURNING id, name, kind
     )
INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date, source, review_status)
SELECT owner.id, utama.id, c.id, c.kind, v.amount, v.notes, NOW() - v.age, 'manual', 'reviewed'
FROM owner, utama, new_categories c
JOIN (VALUES
    ('Investasi', 'expense', 1000000, 'Beli reksa dana contoh', INTERVAL '2 days'),
    ('Investasi', 'income', 20000, 'Hasil investasi contoh', INTERVAL '2 days'),
    ('investasi', 'expense', 75000, 'Kategori huruf kecil', INTERVAL '1 day'),
    ('Invest', 'expense', 50000, 'Kategori yang sudah diganti nama', INTERVAL '1 day'),
    ('Penyesuaian Saldo', 'expense', 10000, 'Penyesuaian contoh', INTERVAL '3 days')
) AS v(name, kind, amount, notes, age) ON v.name = c.name AND v.kind = c.kind;

-- A second synthetic owner with an archived income "Internal Movement" (no figures are read for this owner).
WITH edge AS (
    INSERT INTO users (username, name, password_hash, invite_code)
    VALUES ('edge_contoh', 'Edge Contoh', 'synthetic-not-a-hash', 'SYNTHETIC') RETURNING id
)
INSERT INTO categories (user_id, name, kind, is_archived, is_excluded_from_budget)
SELECT edge.id, v.name, v.kind, v.archived, v.excluded FROM edge, (VALUES
    ('Internal Movement', 'income', TRUE, TRUE),
    ('Internal Movement', 'expense', FALSE, TRUE),
    ('Investasi', 'expense', FALSE, FALSE)
) AS v(name, kind, archived, excluded);
COMMIT;
