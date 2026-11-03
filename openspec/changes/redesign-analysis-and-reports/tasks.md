# Tasks

This plan is pending the owner's approval (proposal.md, "Approval"). Nothing below starts before that approval is recorded with the owner's words and the date.

- **Evidence:** everything a task names goes into `verification.md` of this change: commands, counts, skips with reasons, screenshots and measured values.
- **Data:** synthetic only, for example "Bank Utama", "Dompet Digital", "RDN Contoh", "Bibit Contoh · Reksa Dana A", "Dana Darurat", "Kedai Awan" and "Internet Rumah".
- **Backend tests:** run on a disposable PostgreSQL 16, migrated by Flyway from V1 to the newest version on `main`, with `REQUIRE_TEST_DATABASE=1`, as `.github/workflows/checks.yml` does.
- **Browser matrix:** 1440, 1280, 1024, 768 and 390 px, each in light and dark, in a headless browser on a local production build.
- **Owner testing:** the owner tests nothing before every phase is done (2026-11-03). These checks are the only runtime evidence, and every screenshot is read by the implementing agent.

## 0. Approval and base

- [ ] 0.1 Record the owner's approval with their words and the date in proposal.md, "Approval". Mark OD40–OD48 resolved with the answers. Commit the record separately. Evidence: the commit.
- [ ] 0.2 Rebase `feat/analysis-and-reports` onto `main` after phase 3 (`redesign-transactions-workspace`) has merged. Confirm that `lib/urlState.ts`, `TransactionDrawer`, the `ui-copy-register` spec and its guard test are present. Evidence: `git log --oneline -1 main`, the merge base, and the presence check.

Expected Result: an approved plan on a branch that contains phase 3.

## 1. Baseline

- [ ] 1.1 On the rebased branch, before any change, record:
  - `npm test`, `npm run type-check`, `npm run lint` and `npm run build`, with the first-load JS of `/insights`, `/laporan` and the shared chunks from the build output;
  - the full backend suite with its skips;
  - the median of 20 local calls of `GET /dashboard/analytics` and `GET /dashboard/period-comparison` on a synthetic owner with 5 000 rows over 18 months;
  - axe results with screenshots of `/insights` (both legacy tabs) and `/laporan` at 1280 and 390 px in both themes.

  Evidence: the baseline table in verification.md.

Expected Result: every gate in design D14 has a measured starting value.

## 2. Shared helpers and additive fields

- [ ] 2.1 Move `_waiting` and `_waiting_section` from `cycle_summary.py` to `services/waiting_rows.py`, and `_plan_lines` to `services/plan_occurrences.py` (design, Context). Keep `build_cycle_summary` output identical. Tests: the existing cycle-summary tests pass unchanged, and a fixture comparison of the Beranda response before and after is equal. Evidence: pytest counts.
- [ ] 2.2 Add the `kakeibo` block to `GET /dashboard/period-comparison` (D3, OD41). Tests:
  - the `observed-insights` scenario "Kakeibo buckets with saving transfers";
  - `kakeibo.current` equals analytics `kakeibo` for the same window, in cycle and month;
  - a fixture of today's response keys and values is unchanged apart from the new block;
  - the previous window without data gives `previous: null`.

  Evidence: pytest counts.
- [ ] 2.3 Add `explanations.keluar` and `explanations.belanja` to `GET /dashboard/analytics` with the phase 1 explanation shape (D2). Tests: each total equals its figure, the Belanja lines show gross and the applied offset, and the existing keys are unchanged. Evidence: pytest counts.

Expected Result: Beranda is unchanged, and the comparison and analytics carry exact, additive fields.

## 3. Calendar backend

- [ ] 3.1 Add `services/calendar.py` and `GET /api/dashboard/calendar` (D6). Tests for every `cycle-calendar` scenario under "Calendar Endpoint", "Calendar Days Add Up to Belanja", "Calendar Heat Levels" and "Calendar Upcoming Bills", plus:
  - cycle and month windows;
  - a month whose 31st is clamped by a short cycle;
  - top-ups counted apart from movements;
  - an ambiguous leg counted as waiting;
  - plan statuses `due`, `overdue`, `met` and `skipped`;
  - `upcoming` with and without an expected salary date;
  - 422 for `timeframe` and `cycle_offset`;
  - owner isolation;
  - the statement count of at most 8 for a 31-day and a 28-day window.

  Evidence: pytest counts.
- [ ] 3.2 Measure the median of 20 calendar calls on the 5 000-row owner. Evidence: the number, which must be under 150 ms.

Expected Result: the calendar adds up to Belanja to the rupiah and counts waiting rows as Tinjau does.

## 4. Flow backend

- [ ] 4.1 Add `services/money_flow.py` with one row-assignment function used by both flow and rows (D8, D9), and `GET /api/dashboard/flow`. Tests for every `money-flow-map` scenario under "Flow Endpoint", "Flow Nodes", "Every Account Node Balances", "Negative Balances in the Flow", "Waiting Rows in the Flow" and "Flow Summary", plus:
  - conservation asserted for every account node on every day of the fixture window;
  - each closing equal to `get_accounts_with_balances` for `through` = today;
  - an archived account with rows in the window;
  - a sell from an investment back to cash;
  - a fee row of a movement;
  - stable node order across `through` values;
  - the statement count of at most 10, independent of accounts and days.

  Evidence: pytest counts.
- [ ] 4.2 Add `GET /api/dashboard/flow/rows` (D9). Tests:
  - the "Flow Rows" scenarios;
  - for every link of the fixture, the paged rows add up to the link amount;
  - node requests;
  - opening and remaining links return no rows;
  - 404 for an unknown key and for another owner's account;
  - 422 for a malformed key;
  - the statement count of at most 4.

  Evidence: pytest counts.
- [ ] 4.3 Measure the median of 20 flow calls and 20 rows calls on the 5 000-row owner, plus `analytics` and `period-comparison` against the baseline. Evidence: the numbers. Flow must be under 200 ms, rows under 100 ms, and the other two within 10% of the baseline.
- [ ] 4.4 Run the full backend suite. Evidence: pass, fail and skip counts, each skip with its reason.

Expected Result: every account balances on every replay day, every link's rows equal the link, and nothing of another owner is reachable.

## 5. Analisis frame and Ringkasan

- [ ] 5.1 Rebuild `app/insights/page.tsx` as the frame (D1):
  - the tab list with roving focus, using the `tab`, `day` and `view` parameters through `replaceSearch`;
  - the basis toggle;
  - the stepper through `TopBarControls`;
  - the rolling-window fallback;
  - panels mounted on selection, with Kalender and Peta Aliran loaded dynamically.

  Tests: the `analysis-workspace` scenarios under "Analisis Tabs in the URL" and "Period Basis for All Analisis Tabs", including switching from `/insights?tab=aliran`. Evidence: Vitest counts.
- [ ] 5.2 Build Ringkasan (D2–D5):
  - the figure strip with explanations;
  - the Kakeibo card;
  - Belanja harian with its table;
  - the restyled `RecurringCharges`, `QuestionRate`, `CashFlowHistory` and `PeriodComparison`;
  - `CategoryBudgetSection` moved with unchanged mutations;
  - the one-column order at a 900 px main column, through the width hook;
  - the phone order.

  Remove `SpendingHeatmap`, and remove `BurnCadenceChart` if nothing else imports it. Tests: the "Ringkasan Figures and Names", "Ringkasan Kakeibo Comparison", "Ringkasan Daily Spending" and "Ringkasan Supporting Cards" scenarios, plus the existing category and budget tests moved with the section. Evidence: Vitest counts.

Expected Result: Ringkasan shows the same Keluar as Beranda and the same Belanja as Transaksi, and category management works as before.

## 6. Kalender

- [ ] 6.1 Build the Kalender tab (D7): the progress band, the grid keyboard model and cell names, the legend with thresholds, the day sheet (ledger rows through the shared drawer, waiting links and plans), the upcoming list, and the phone week strip. Tests: the "Kalender Grid", "Kalender Day Sheet" and "Kalender Phone Week Strip" scenarios, plus `vitest-axe`. Evidence: Vitest counts.

Expected Result: every day is reachable by keyboard and names its figures, and the phone has its own composition.

## 7. Peta Aliran

- [ ] 7.1 Port the preview's `sankey.js` to `components/analysis/flow/sankey.ts` (D10, phase plan D21). Tests: deterministic node and link positions for a fixture, no overlapping nodes in a column, link order, a single-account period, an empty period, and the width range 320–1440. Evidence: Vitest counts.
- [ ] 7.2 Build the tab (D10): the map with patterns and the node keyboard model, the table view (`view=`), the highlight chips, the fold with a per-viewer preference, replay with prefetch and reduced motion, the drawer with paged rows into `TransactionDrawer`, and the summary figures. Tests: the "Peta Aliran Map and Keyboard", "Peta Aliran Table", "Peta Aliran Fold" and "Peta Aliran Replay" scenarios, plus `vitest-axe`. Evidence: Vitest counts.

Expected Result: everything the map shows is reachable by keyboard and complete in the table.

## 8. Laporan

- [ ] 8.1 Restyle `/laporan` (D11):
  - the basis toggle and report stepper in the top bar;
  - `replaceSearch` for `basis` and `start`;
  - the main and side columns with "Laporan sebelumnya";
  - the restyled report components;
  - the native dialogs for classification and budget;
  - the states.

  Tests: the `monthly-report` "Report page layout" scenarios, including "Stepping to the previous report" from a URL with a query; `lengkapi=1` and `atur=anggaran` still open their dialogs. Evidence: Vitest counts.
- [ ] 8.2 Add `lib/reportLead.ts` and render the lead (D12). Table-driven tests for the "Report Written Lead" and "Report Lead Register" scenarios: complete, incomplete, no comparison, unavailable figure, hidden balances, no verdict words, and no "kamu", "Anda" or slang. Evidence: Vitest counts.

Expected Result: Laporan reads as a report with a neutral lead, and every existing link and dialog still works.

## 9. Copy, docs and frontend checks

- [ ] 9.1 Sweep `/insights` and `/laporan` labels to the neutral register (D13), including formal "Anda". Extend the phase 3 copy guard to the new files. Evidence: the guard test passes, and the list of renames is in verification.md.
- [ ] 9.2 Update `docs/NEW_USER_GUIDE.md` (the three tabs and the Laporan lead). Evidence: the diff.
- [ ] 9.3 Run `npm ci`, `npm run type-check`, `npm run lint` (0 errors, 0 warnings), `npm test` and `npm run build`. Record the `/insights`, `/laporan`, Kalender and Peta Aliran chunk sizes and the shared first-load JS against D14. Evidence: counts and sizes.

Expected Result: the frontend passes as CI runs it, and the bundle gates hold.

## 10. Browser verification

- [ ] 10.1 Run the browser matrix on `/insights?tab=ringkasan`, `?tab=kalender` (with a selected day), `?tab=aliran` (map and table, folded and unfolded, a node drawer and a link drawer) and `/laporan` (a complete report and the new-user state). For every cell record:
  - axe with 0 serious or critical violations;
  - horizontal overflow of 0;
  - 0 console errors;
  - the lowest measured text contrast;
  - targets of at least 24 px;
  - focus not obscured.

  Also check 200% and 400% zoom on each tab, and reduced motion on Peta Aliran. Evidence: results JSON and screenshots, each read by the agent.
- [ ] 10.2 Real-UI checks on the seeded owner:
  - Ringkasan's Keluar equals Beranda's and its Belanja equals Transaksi's;
  - the calendar's day amounts add up to Belanja (read from the page);
  - for each account node in the table, the in and out sums are equal;
  - a link drawer's rows add up to the link;
  - replay from day 1 to today with the map stable and the day announced;
  - answering the waiting leg in Tinjau moves it out of "Menunggu pasangan" on the next load;
  - keyboard walks of the tab list, the calendar grid and the map nodes;
  - Laporan stepping back and forward with the URL updated from a URL with a query.

  Evidence: the interaction JSON and screenshots.
- [ ] 10.3 Regression pass on `/`, `/review`, `/ledger`, `/accounts`, `/goals` and `/settings`, against a build of `main` at the merge base. Evidence: per route, axe, overflow and console results for both builds.

Expected Result: both pages meet WCAG 2.2 AA in the matrix, the money figures agree across pages, and no other route regressed.

## 11. OpenSpec and diff

- [ ] 11.1 `openspec validate --all --strict` exits 0, and `git diff --check` is clean. Evidence: the output lines.
- [ ] 11.2 Fill verification.md, including "Not verified" with reasons. Push `feat/analysis-and-reports` so CI runs. Open no pull request. Evidence: the CI run id and its result.

Expected Result: a pushed branch with green CI and a complete verification record.

## 12. Acceptance

- [ ] 12.1 Present verification.md, the key screenshots, the deviations and any new open decisions to the owner. Record the acceptance with the owner's words and the date in proposal.md. The owner accepts on this evidence, because they test only after all phases are done. Evidence: the owner's words quoted with the date.

## Workflow follow-up

- After acceptance:
  - run `openspec archive redesign-analysis-and-reports -y` on the branch;
  - confirm `openspec validate --all --strict` exits 0;
  - push.
- The owner opens and merges the pull request. The release runs only through `deploy.yml`.
- After the deploy, check production read-only:
  - health;
  - the container build times;
  - `GET /api/dashboard/calendar` and `/flow` answer 401 without a session, which shows the routes exist;
  - the frontend build contains `/insights`.
- Delete the branch locally and on the remote.
