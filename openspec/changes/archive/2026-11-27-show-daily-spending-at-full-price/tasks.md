## 1. Backend

- [x] 1.1 `spending_basis` builds days, day pillars and day categories from full-price rows; total, `by_category`, pillars and `reimbursed` unchanged; `day_categories` lists every category with spending that day. Evidence: tests "An unlinked reimbursement leaves the days alone" and "A linked reimbursement leaves its day alone"; days add up to total + reimbursed; existing totals tests pass unchanged.
- [x] 1.2 Remove `daily_offsets`, `offset_day_shares`, `SpendingOffsets.linked_days`, `linked_by_category_pillar_day` and the loader's per-day column, with their tests. Evidence: `rg` finds no reference; reimbursement tests pass with unchanged totals.
- [x] 1.3 Calendar: full-price days and heat, `totals.spending_gross` and `totals.spending_reimbursed`, consistency checks for the new sums. Evidence: tests "Reimbursement on a later day", "A day matches its rows"; Kalender day equals the analytics day.
- [x] 1.4 Analytics and overview: full-price `daily_cadence`, heatmap, trendline and burn cadence; `spending_totals`. Evidence: test "Days add up to the gross outflow"; `total_outflow` and `projected_30d_outflow` unchanged.

Expected Result: every day equals its rows; no period figure changes.

## 2. Frontend

- [x] 2.1 Kalender hero line and Ringkasan daily chart line "Rp A dibelanjakan · −Rp B diganti" (id and en), only when something was reimbursed, masked with hidden balances; types updated. Evidence: component tests for both cases and for masking; type check, lint, tests and build.

Expected Result: where days and the period total appear together, the difference is named.

## 3. Verify and deliver

- [x] 3.1 Full backend and frontend suites; `openspec validate --all --strict`; browser check of Kalender and Ringkasan on a production build with synthetic data at 1440 and 390 px, light and dark, axe and console; `verification.md`.
- [x] 3.2 After the owner accepts: archive, push, open the PR and report its URL.
