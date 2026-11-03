-- Prepared rollback companion for redesign-review-triage (design.md, "Migration Plan"). Not applied:
-- Flyway reads only db/migrations. Use it only when the release that added V44 is reverted.
--
-- Why: the reverted code does not sweep review_actions, so undo snapshots of the last minutes before the
-- rollback (merchants, notes and amounts) would outlive their ten-minute window. Copy this file into
-- db/migrations under the next free version (V45 if still free) in the revert's pull request, so the
-- pipeline runs it before the old API starts. The table and V44's column stay; a forward release can
-- reuse the empty table. No money figure depends on review_actions.
DELETE FROM review_actions;
