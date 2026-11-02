# Proposal

## Why

The owner does not read a monthly report: "for the report and analysis which I want to read monthly still not effective for me either it's also from me that not consistent enough to read it". An audit on 2026-11-02 found that the figures reconcile exactly with the ledger, so the gap is elsewhere. There is no single monthly report, and nothing prompts anyone to read one. 34 of 120 October expenses had no Kakeibo pillar and were silently counted as "need". 0 of 13 categories have a budget, yet budget columns fill the Analisis page. The product is public and multi-user, so the report must work for any user, not only the owner.

## Approval

On 2026-11-02 the owner approved the audit recommendations ("ok I approve all you recommend") and the start of Phase 0. The decisions that apply here are:
- scope is the personal book only;
- optional features stay hidden until needed;
- amounts are never guessed;
- the phone companion is a tracker with no financial dashboards, so the monthly reminder is built later on the phone from a server API.

Recorded 2026-11-02.

## Expected Outcome

- A user opens `/laporan` and reads one report per calendar month, or per salary cycle when they have confirmed salary receipts. The report answers the four Kakeibo month-end questions in a fixed order, compares the period with the previous one and with the median of three earlier periods, shows a "Kualitas data" box, and keeps a short reflection note for the next month. Every amount opens the ledger filtered to the rows behind it.
- The report's inflow, outflow, net and per-pillar amounts equal the ledger summary for the same window.
- Spending without a pillar appears as "Belum diklasifikasi" wherever the Kakeibo split is shown, instead of being counted as "Kebutuhan". "Lengkapi klasifikasi" lets the user set a pillar per category or for one or many transactions. Each correction is written to `classification_feedback`, as the existing owner edits are.
- A user without any budget sees no budget columns or figures. "Atur anggaran" in the report sets per-category monthly budgets. Once a budget exists, the report shows budget against actual.
- `GET /api/reports/monthly/latest` tells the future phone reminder whether a closed period's report is ready, with its label, window, the pending Tinjau count and the web path.

## What Changes

- New backend reporting service and routes:
  - `GET /api/reports/monthly`: the report for one period;
  - `GET /api/reports/monthly/periods`: the list of past reports;
  - `GET /api/reports/monthly/latest`: readiness for the phone reminder;
  - `PUT` and `DELETE /api/reports/monthly/reflection`: the reflection note.

  All of them use session or Bearer auth and are owner-scoped. The report is built from `report_figures`, `observed_insights`, `essential_cost`, `cash_flow_history` and `recurring_charges`. It adds no calculation rule that could disagree with the ledger.
- New `monthly_reflections` table through Flyway migration `V41__monthly_reflections.sql`, scoped by owner, period basis and period start.
- **BREAKING (meaning, not shape)**: the effective Kakeibo pillar of an expense with no transaction or category pillar becomes `null` ("Belum diklasifikasi") instead of `need`. Kakeibo breakdowns gain `unclassified_spent` and `unclassified_pct`. Comparison pillar lists gain an `unclassified` entry. Ledger `kakeibo_type` filters accept `unclassified`. Category lists return a stored pillar or `null` instead of deriving one from `is_primary`. A transaction recorded in a category without a pillar no longer gets `need` or `want` copied from `is_primary`. Existing pillar values and field names are unchanged.
- The essential monthly cost keeps counting unclassified spending, labelled as such, so Ketahanan Dana does not grow because a pillar is missing (design.md D6).
- New classification quick-fix routes:
  - `GET /api/classification/gaps`;
  - `POST /api/classification/categories/{id}`;
  - `POST /api/classification/transactions`.

  They change rows only through the owner-edit path and write `classification_feedback` rows.
- The ledger list accepts `possible_duplicates=true`: same account, amount and type within 3 minutes, movements excluded.
- Web:
  - the `/laporan` page, with a "Laporan" entry in the navigation and a link from Analisis;
  - the "Lengkapi klasifikasi" and "Atur anggaran" dialogs;
  - a sixth "Belum diklasifikasi" bucket in the Kakeibo views;
  - budget UI on Analisis hidden while no budget exists;
  - forms that no longer turn an unclassified row into "Kebutuhan" on save.

## Non-goals

- The phone reminder itself, and any Android change. The companion only reads server figures; design.md D3 records that it accepts the changed values.
- Business, freelance or enterprise books.
- Typed-capture defaults in `backend/app/services/capture_*`. Another agent owns those files; they are recorded as a follow-up.
- New savings, coverage or comparison formulas.
- Historical balances for past coverage. Coverage in the report is labelled "saat ini".
- Production access or release. All verification is local, on disposable PostgreSQL with synthetic data. Pushing the branch is authorised. The pull request, merge and release stay with the owner and the approved pipeline.

## Capabilities

### New Capabilities

- `monthly-report`: the per-period report, its sections, comparison, data-quality box, drill-down, reflection note, budget section and the readiness API.
- `kakeibo-classification-completeness`: the "Belum diklasifikasi" bucket, its filters and the quick-fix flow with feedback.

### Modified Capabilities

- `kakeibo-decoupling`: the effective pillar no longer defaults to `need`.
- `kakeibo-reconciliation`: allocations include the unclassified bucket, and the classifications are shown individually with it.
- `kazz-ui-and-kakeibo`: classifications are visible individually, with the sixth bucket.
- `financial-reporting-consistency`: `top_categories` pillars may be `null` for a category without one.
- `observed-insights`: the fair comparison reports the unclassified bucket.
- `emergency-cost-estimation`: unclassified spending counts in essential spending and is shown as such.
- `clean-core-ledger`: `is_primary` no longer derives a pillar when a category or transaction is read or recorded.
- `custom-category-management`: optional budgets are hidden until one exists.

Standards:
- database: an additive migration with rollback in design.md;
- money: no new formula, and reconciliation tests tie the report to the ledger;
- multi-user: every route is owner-scoped and has isolation tests;
- api-consumers: additive fields, with Android compatibility in design.md D3;
- personal-data: the reflection note is owner-only and never logged.

No new project capability is added.

## Impact

- Backend:
  - `services/kakeibo.py`, `report_figures.py`, `observed_insights.py`, `essential_cost.py`, `reimbursements.py`, `history_suggestions.py` and `text_capture.py` (pillar SQL only);
  - routers `dashboard.py`, `transactions.py`, `categories.py` and `review.py`;
  - new `services/monthly_report.py`, `services/classification_gaps.py`, `routers/reports.py`, `routers/classification.py`, and `main.py` registration.
- Database: `db/migrations/V41__monthly_reflections.sql`.
- Web:
  - new `app/laporan/` and `components/report/`;
  - `lib/kakeibo.ts`, `lib/ledgerFilters.ts`, `tokens.css` (a neutral unclassified token), `KakeiboBenchmark`, `MobileHomeView`, `ObservedInsights`, `insights/page.tsx`, `ledger/page.tsx`, `Sidebar`, `BottomNav`.
- Specs: the two new capabilities and the eight modified ones listed above.
- Android: none (design.md D3).
