## 1. Report windows and figures

- [x] 1.1 Move the timeframe windows to `services/report_windows.py` and add the calendar month (`timeframe=month`, previous month, Indonesian label); dashboard accepts it (design D5).
  - Evidence: `test_calendar_month_window` (four boundary cases incl. leap February and year change), `test_month_timeframe_and_ledger_period_share_one_window`; existing `test_get_timeframe_window_options` and cycle tests pass.
- [x] 1.2 `services/report_figures.py`: reportable expense/income predicates and the category breakdown with "Tanpa kategori" and archived categories; overview and analytics use it (D6).
  - Evidence: `test_breakdown_adds_up_to_total_outflow_with_uncategorised_and_archived` (Σ spent = total outflow, bucket Rp 70.000, archived Rp 30.000, no Investasi/Internal Movement).
- [x] 1.3 Attribute applied offsets to days; net daily series, heatmap and burn rates in analytics and overview (D9).
  - Evidence: `test_series_heatmap_burn_and_ledger_follow_reimbursement_offsets`, `test_offset_is_spread_over_the_days_the_category_was_spent`, `test_apportion_is_exact_and_never_exceeds_a_weight`; mocked dashboard tests updated to the new query order.

Expected Result: every report figure uses one spending rule, categories add up to the total, and series agree with the totals.

## 2. Ledger API

- [x] 2.1 `period`, `period_offset` and `uncategorized` filters with explicit 422 conflicts; summary `period` block (D5, D7).
  - Evidence: `test_month_timeframe_and_ledger_period_share_one_window` (only the in-window row, label, 422 `period_with_dates`, 422 for `period=week`), `test_uncategorized_filter_lists_the_bucket_rows`.
- [x] 2.2 Reimbursement-net ledger outflow with `gross_outflow` and `reimbursement_offset`, only for report-shaped filters (D9).
  - Evidence: `test_series_heatmap_burn_and_ledger_follow_reimbursement_offsets` (unfiltered, category, pillar, account cases); `test_analytics_insights_and_ledger_follow_the_same_figures` updated from gross to net (phase 2 open decision 6).
- [x] 2.3 `movement_fee` and `fee_movement` link fields (D8).
  - Evidence: `test_fee_rows_and_their_movement_link_each_other`.
- [x] 2.4 Confirm the purpose answer endpoint works with a browser session and the origin check (D1 context).
  - Evidence: `test_answer_endpoint_accepts_a_browser_session_with_origin_check` (403 without Origin and with a foreign Origin, 200 with the app origin, feedback written).

Expected Result: the ledger can be opened on exactly the rows behind a report figure, and its summary agrees with the dashboard.

## 3. Review API

- [x] 3.1 `GET /api/review/summary` and `GET /api/review/queue` with sections, grouping and open reasons (D1–D3).
  - Evidence: `test_queue_lists_every_section_and_the_badge_counts`, `test_queue_groups_merchant_less_rows_by_signature_and_movements_apart`, `test_resolved_reasons_are_not_shown`, `test_keep_separate_stops_automatic_pairing` (sends move to awaiting).
- [x] 3.2 `POST /api/review/bulk`: owner-scoped, validated, one transaction, max 200, owner-edit hooks, movement legs (D4).
  - Evidence: `test_accept_marks_rows_reviewed_without_reclassifying`, `test_recategorise_writes_one_correction_per_row_and_teaches_memory` (2 feedback rows, memory, idempotent repeat), `test_rename_sets_merchant_and_notes_with_feedback`, `test_recategorise_rejects_a_category_of_the_other_kind`, `test_foreign_rows_reject_the_whole_batch`, `test_movement_legs_accept_but_never_reclassify`, `test_keep_separate_stops_automatic_pairing`, `test_invalid_bulk_bodies_are_rejected` (9 bodies), `test_batch_of_two_hundred_rows_is_accepted`.

Expected Result: the owner can confirm or correct a whole merchant at once, and every correction teaches memory exactly like a single edit.

## 4. Web

- [x] 4.1 `/review` page with the four sections, group actions, awaiting completion and ambiguous linking; navigation entry with count badge (sidebar, drawer, phone menu button) (D1, D4, open decisions 1 and 9).
  - Evidence: `src/app/review/page.test.tsx` (7 tests incl. axe on the full and empty page, keyboard accept, chip and text answers, recategorise and rename payloads, completion and merge payloads, failure without false success); `ReviewNavBadge.test.tsx`; `lib/review.test.ts`.
- [x] 4.2 Ledger: URL filters, period selector and stepper, active filter chips, review marks, fee links, `?edit=` deep link, reimbursement note (D7, D8).
  - Evidence: `src/app/ledger/page.test.tsx` "Ledger filters from the URL" (API params from the URL, chips, period label, marks, fee text, axe, stepper URL, uncategorised mapping, deep link returns to `/review`); `lib/ledgerFilters.test.ts`; existing ledger tests pass.
- [x] 4.3 Insights and home: cycle/month switch, "Tanpa kategori" and archived rows without management actions, category and pillar drill-down links (D6, D7).
  - Evidence: `src/app/insights/page.test.tsx` "report drill-down" (hrefs, archived label, no actions, axe, switch, pillar link); existing insights tests pass.
- [x] 4.4 Browser check against seeded synthetic data at 390 and 1280 px, light and dark: every section rendered, no horizontal overflow, keyboard through group actions and the answer form, deep link, badge, 0 console errors, axe on the rendered pages.
  - Evidence: `verification.md` (browser section).

Expected Result: the owner reviews everything on one page by mouse or keyboard, and every report figure opens its rows.

## 5. Verification

- [x] 5.1 Backend suite on a freshly recreated PostgreSQL migrated V1→V30; frontend tests, lint, type-check and build; `openspec validate` (change, `--all`, `--strict`).
  - Evidence: `verification.md` with counts and exit codes.

Expected Result: all suites green on a fresh database and the specs validate.

## Workflow follow-up

- Archive with `openspec archive add-review-queue-and-report-fixes --yes`, re-run `openspec validate --all`.
- Commit, push `feat/review-queue-and-reports` and report the compare link; the owner merges.
