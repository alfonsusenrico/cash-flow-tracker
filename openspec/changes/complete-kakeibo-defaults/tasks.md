# Tasks

## 1. Branch update

- [x] 1.1 Merge `origin/main` (7fcd266) into `feat/monthly-report` with a merge commit. Keep both sides of `AGENTS.md` (the `/laporan` references and the companion release path) and both behaviours in `text_capture.py` (no `need` fallback, plus `time_basis`/`time_evidence`). Evidence: merge commit; no conflict markers.
- [x] 1.2 Renumber `V40__monthly_reflections.sql` to `V41__monthly_reflections.sql` and update the references in `openspec/specs/monthly-report`, the archived `add-monthly-report` notes and its delta spec. Evidence: no duplicate Flyway version in `db/migrations`; Flyway V1→V41 on a fresh PostgreSQL 16.

Expected Result: the branch contains main, with one migration per version.

## 2. Defaults

- [x] 2.1 D1: confirm the essential cost counts unclassified spending, labelled. No change. Evidence: `test_essential_cost_still_counts_unclassified_spending_and_reports_it`.
- [x] 2.2 D2: `PATCH /api/categories/{id}` writes feedback like "Lengkapi klasifikasi". Evidence:
  - `test_category_edit_pillar_change_writes_feedback_like_lengkapi_klasifikasi`;
  - `test_category_edit_without_a_pillar_change_writes_no_feedback`;
  - `test_category_edit_of_a_non_reportable_category_writes_no_feedback`;
  - `test_category_edit_is_owner_scoped_and_writes_nothing_for_another_owner`;
  - a loopback API run: 1 correction, then 0 on repeat.
- [x] 2.3 D3: no silent `need` in category creation and typed capture, and the Android null-pillar check. Evidence:
  - `test_category_created_without_a_pillar_stays_unclassified[True/False]`;
  - `test_category_created_with_a_pillar_keeps_it`;
  - `test_typed_capture_in_a_category_without_a_pillar_stores_no_pillar`;
  - `test_typed_capture_keeps_a_pillar_the_owner_chose`;
  - `test_capture_normalization_leaves_a_pillarless_category_unclassified`;
  - the `mobile/` source review in design.md D3.
- [x] 2.4 D4: starter pillars for new users only. Evidence: `test_new_user_starter_categories_carry_default_pillars`, `test_existing_categories_keep_their_pillars_when_defaults_are_seeded_again`, and a registration through the loopback API that listed the mapped pillars.
- [x] 2.5 D5: dock tokens and `BottomNav` contrast. Evidence:
  - `tokens.test.ts` dock pairs in both themes, which fail at 2.2:1 and 2.6:1 with the old ink;
  - the `BottomNav` source check;
  - headless Brave at 390 px.

Expected Result: all five defaults behave as approved. The new tests fail on the previous code (8 of 12) and pass now.

## 3. Verification

- [x] 3.1 Backend suite in a Python 3.12 venv with `backend/requirements-dev.txt` (pytest 9.0.3), on a fresh PostgreSQL 16 database migrated V1→V41 by Flyway, with `SKIP_TEST_MIGRATIONS=1` and `REQUIRE_TEST_DATABASE=1` as in `checks.yml`. Evidence: counts in verification.md.
- [x] 3.2 Frontend `npm ci`, test, lint, type-check and build. Evidence: counts in verification.md.
- [x] 3.3 Headless Brave on `/laporan` and the bottom navigation at 390 px, light and dark. Evidence: verification.md.
- [x] 3.4 `git diff --check` and `openspec validate --all --strict`. Evidence: exit 0.
- [ ] 3.5 Push `feat/monthly-report` (normal push) and report the Checks workflow result and the compare link.

Expected Result: local checks pass, the branch is pushed, and CI reports its result.
