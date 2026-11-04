-- Accounts reported by more than one app (reconcile-linked-account-reports D1, D7).
-- Additive: one table, one nullable column with a partial index, then the skip patterns are converted
-- into links and stopped. An older release ignores the table and the column; it reads the stopped
-- patterns as inactive, so it asks in Tinjau again. Rollback: revert the release and leave the table,
-- the column and the stopped patterns in place, unused. Every statement is safe to run twice.

-- 1. "This account is also reported by this institution's app", learned from the owner's answer only.
CREATE TABLE IF NOT EXISTS account_report_links (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    institution TEXT NOT NULL CHECK (institution IN ('bca', 'jago', 'gopay', 'shopeepay', 'stockbit')),
    source TEXT NOT NULL CHECK (source IN ('confirmation', 'converted')),
    converted_from_pattern_id UUID NULL REFERENCES notification_skip_patterns(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    stopped_at TIMESTAMPTZ NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS account_report_links_one_active
    ON account_report_links (user_id, account_id, institution) WHERE stopped_at IS NULL;

-- 2. The link that skipped an event; NULL for every other event, and when the link is deleted.
ALTER TABLE notification_events ADD COLUMN IF NOT EXISTS reporting_link_id UUID NULL
    REFERENCES account_report_links(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS notification_events_link_skipped
    ON notification_events (user_id, post_time) WHERE reporting_link_id IS NOT NULL;

-- 3. Each active skip pattern becomes the link of the one account its kept app's notification names
--    resolve to, with the pattern's redundant app (OD132). A pattern that resolves to no account or to
--    several converts to nothing; its next mirror asks once in Tinjau.
WITH kept_accounts AS (
    SELECT p.id AS pattern_id, p.user_id, p.redundant_institution, MIN(a.account_id::text)::uuid AS account_id
    FROM notification_skip_patterns p
    JOIN notification_account_aliases a ON a.user_id = p.user_id AND a.institution = p.kept_institution
    JOIN accounts acc ON acc.id = a.account_id AND acc.user_id = a.user_id AND acc.is_archived = FALSE
    WHERE p.stopped_at IS NULL
    GROUP BY p.id, p.user_id, p.redundant_institution
    HAVING COUNT(DISTINCT a.account_id) = 1
)
INSERT INTO account_report_links (user_id, account_id, institution, source, converted_from_pattern_id)
SELECT k.user_id, k.account_id, k.redundant_institution, 'converted', k.pattern_id
FROM kept_accounts k
ON CONFLICT (user_id, account_id, institution) WHERE stopped_at IS NULL DO NOTHING;

-- 4. Reports a converted pattern skipped now name its link, so "Dilewati otomatis" keeps listing them.
UPDATE notification_events n
SET reporting_link_id = l.id
FROM account_report_links l
WHERE l.converted_from_pattern_id = n.skip_pattern_id AND l.user_id = n.user_id
  AND n.skip_pattern_id IS NOT NULL AND n.reporting_link_id IS NULL;

-- 5. Patterns no longer act: every active one is stopped, converted or not.
UPDATE notification_skip_patterns SET stopped_at = NOW() WHERE stopped_at IS NULL;
