# Tasks

## 1. Kakeibo completeness in the backend (D3, D6)

- [x] 1.1 Add `UNCLASSIFIED`, `REPORT_BUCKETS`, `pillar_bucket_sql` and `PILLAR_FILTER_PATTERN` to `services/kakeibo.py`. Make `effective_pillar_sql` and `effective_pillar` return no pillar instead of `need`. Replace every hand-copied `'need'` fallback with the shared helpers. Evidence: `rg "'need'" backend/app` leaves only the explicit fee-category pillar, the `need`-budget fallback (D6), the history-suggestion supersede comparison (Risks), and the capture files owned by the concurrent change (open decision 3). Covered by `test_kakeibo_completeness.py`.
- [x] 1.2 Add `unclassified_spent`/`unclassified_pct` to the Kakeibo breakdown, an `unclassified` entry to the comparison pillars, and an `unclassified` key to reimbursement offsets. Evidence: tests showing that six buckets add up to outflow, with offsets in a mixed category.
- [x] 1.3 Make the essential cost count `need` + unclassified with `unclassified_amount` per period. Evidence: an unchanged essential cost for unclassified rows, and the period share reported.
- [x] 1.4 Stop categories reads, transaction create and recategorise from deriving a pillar from `is_primary`. Evidence: tests showing a category without a pillar listed with `null` and its new row stored without a pillar. Negative: an explicit category pillar is still copied.
- [x] 1.5 Accept `kakeibo_type=unclassified` and `possible_duplicates=true` in the ledger list, with the summary offsets for the bucket. Evidence: tests for the filter sum, the duplicate rows (movement leg excluded) and an invalid value (422).
- [x] 1.6 `classification` routes: gaps, category fix and row fix, with feedback and owner scope. Evidence: tests for feedback rows (before `null`, after pillar), for nothing written when nothing changes, for 404 on another owner's rows, and for 409/422 on movement and income rows.

Expected Result: missing pillars are reported as "Belum diklasifikasi" everywhere and can be fixed with feedback, while totals and the essential cost are unchanged.

## 2. Monthly report backend (D1, D2, D4, D5, D8, D9, D10)

- [x] 2.1 Migration `V40__monthly_reflections.sql`. Evidence: Flyway V1→V40 on a fresh database.
- [x] 2.2 `services/monthly_report.py` with periods, sections, comparison, median, data quality and budget. Evidence: section tests on a synthetic two-month owner.
- [x] 2.3 `routers/reports.py` with report, periods, latest and reflection routes. Evidence:
  - the report-equals-ledger reconciliation test;
  - a reflection CRUD test with owner isolation;
  - latest-period tests (session equals Bearer, `ready: false`, 401);
  - budget hidden or shown;
  - 404/409/422 paths.

Expected Result: the report API answers the four questions with figures equal to the ledger.

## 3. Web (D3, D10, D11)

- [x] 3.1 `lib/kakeibo.ts` unclassified bucket and token; `ledgerFilters` `kakeibo=unclassified` and `dup=1`; ledger chips. Evidence: unit tests.
- [x] 3.2 Sixth bucket in `KakeiboBenchmark`, `MobileHomeView`, `ObservedInsights` and `EmergencyCoverage`; ledger and category forms keep unclassified values. Evidence: component tests.
- [x] 3.3 `/laporan` page and `components/report/*`, with navigation entries. Evidence: page tests with axe.
- [x] 3.4 "Lengkapi klasifikasi" and "Atur anggaran" dialogs; the Analisis budget UI hidden without budgets. Evidence: component tests with axe.
- [x] 3.5 Browser check in headless Brave with synthetic data covering two months, unclassified items and a duplicate. It covers `/laporan` at 390 and 1280 px, light and dark, with screenshots, a keyboard pass, overflow, console errors and axe. Evidence: verification.md.

Expected Result: a user reads the report on a phone, fixes classification and sets budgets from it.

## 4. Verification and delivery

- [x] 4.1 Backend on a fresh PostgreSQL 16 database migrated V1→V40, plus a CI-style run without a database. Evidence: counts in verification.md.
- [x] 4.2 Frontend `npm ci`, test, lint, type-check and build. Evidence: counts in verification.md.
- [x] 4.3 Android compatibility, read-only. Evidence: design.md D3.
- [x] 4.4 `git diff --check`, `openspec validate --all --strict` and a final diff review. Evidence: verification.md.
- [x] 4.5 Commit, archive on the branch and push `feat/monthly-report` without opening a pull request. Evidence: `git log` and the pushed branch.

Expected Result: verified, archived and pushed, ready for owner review.
