# Proposal: fix-monthly-report-accuracy

## Approval

Approved by the owner on 2026-11-27 ("ok, do all") after the production cycle audit exposed report-default, top-places, search-link, cycle-boundary, partial-status, and false-duplicate defects.

## Why

When a new salary cycle closes, the sidebar report badge points to that cycle, yet opening `/laporan` shows the calendar month instead. Inside the report, "Tempat belanja terbesar" omits every expense whose `merchant` column is null (such as manual entries and descriptions without a separate merchant), leaves linked reimbursements unsubtracted so a fully repaid pass-through can top the list, and can emit broken ledger search text when a merchant name contains formatted digits or invisible Unicode characters. In addition, a closed salary cycle's pre-payroll closing hours are falsely flagged as a day without records, the earliest partial period is labelled "Lengkap" in the past-reports rail, and the duplicate detector flags distinct same-amount rows from different counterparties or categories within 3 minutes.

## Expected Outcome

- Opening `/laporan` without `?basis=` selects `cycle` when the owner has confirmed salary cycles (matching `GET /api/reports/monthly/latest` and the sidebar badge) and `month` otherwise, while explicit `?basis=month` or `?basis=cycle` always wins.
- "Tempat belanja terbesar" and the top-merchant fact include expenses identified by `name` when `merchant` is null, subtract linked reimbursement allocations (`reimbursement_links`) per expense row, strip invisible format characters and orphaned punctuation from `search_text` so every row link finds its transactions, and guard typed captures so the capture's own amount does not leak into a model-proposed merchant name.
- Closed salary cycles check only complete local days for `days_without_records`, never flagging the partial closing morning before the next payroll receipt.
- The past-reports list labels a closed period where tracking started mid-period as "Belum lengkap" ("Partial" in English), matching the hero notice.
- `possible_duplicate_sql` stops flagging same-amount transactions within 3 minutes when their non-empty labels or non-null categories clearly differ.

## What Changes

- **Laporan default basis:** Update `frontend/src/app/laporan/page.tsx` so omitting `basis` selects `cycle` when `cycle_available` is true and `month` otherwise, keeping explicit URL parameters authoritative.
- **Top spending places and search links:** Update `merchant_spending` and `search_text` / `merchant_key` in `backend/app/services/observed_insights.py` (and `spending_merchants` in `backend/app/services/spending_basis.py`) to fall back to non-placeholder `name` when `merchant` is empty, subtract linked `reimbursement_links` allocations per expense row (floored at 0, dropping fully reimbursed entries), and strip invisible format characters plus orphaned separators from `search_text`.
- **Capture merchant sanitization:** Update `validate_capture_normalization` in `backend/app/services/capture_normalization.py` to strip trailing/embedded tokens matching the capture's parsed amount from a model-proposed `merchant` (and matching description tail) so amounts never become part of the merchant name.
- **Salary-cycle empty-day boundary:** Update `_data_quality` in `backend/app/services/monthly_report.py` so a closed period whose `last_instant` ends before the end of its closing local day excludes that partial closing day from `days_without_records`.
- **Partial first-period label:** Update `PastReports` in `frontend/src/components/report/ReportContext.tsx` and both message catalogues (`id.json`, `en.json`) to show "Belum lengkap" / "Partial" when `complete && tracking_started_inside`.
- **Duplicate detection precision:** Update `possible_duplicate_sql` in `backend/app/services/report_figures.py` so two same-account, same-type, same-amount rows within 3 minutes are flagged only when neither their normalized non-empty labels (`COALESCE(NULLIF(BTRIM(merchant),''), NULLIF(BTRIM(name),''))`) nor their non-null categories contradict each other.

## Non-goals

- Changing period totals (`Pengeluaran`, `Belanja`, `Masuk`, `Ditabung & investasi`, Kakeibo pillars, or category totals), which remain unchanged.
- Subtracting unlinked category-only reimbursements from individual merchants (unlinked reimbursements do not identify a specific expense or merchant).
- Daily full-price display on Kalender and Ringkasan (handled in `show-daily-spending-at-full-price`).
- Notification account mapping or Stockbit noise filtering (handled in `fix-notification-mapping-and-stockbit-noise`).
- Direct production database writes or manual backfills of historical transactions; verification runs strictly locally against disposable PostgreSQL and synthetic fixtures, and production changes ship only through the approved pipeline after owner acceptance.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `monthly-report`: default web report basis follows `cycle_available`; top spending places fall back to expense names, subtract linked reimbursements, and emit clean searchable links; closed salary cycles exclude the partial closing day from `days_without_records`; past-reports rail labels partial first periods consistently; `possible_duplicate_sql` excludes rows with clearly distinct labels or categories.
- `text-capture`: model-proposed merchant names strip tokens matching the capture's parsed money expression.

No project capability is added; existing `money`, `database`, and `llm` standards apply.

## Impact

- **Backend:** `backend/app/services/observed_insights.py`, `backend/app/services/spending_basis.py`, `backend/app/services/monthly_report.py`, `backend/app/services/report_figures.py`, `backend/app/services/capture_normalization.py`, and targeted backend tests.
- **Frontend:** `frontend/src/app/laporan/page.tsx`, `frontend/src/components/report/ReportContext.tsx`, `frontend/src/messages/{id,en}.json`, and frontend tests.
- **API & Data:** No database migration and no breaking response-shape changes. Existing fields keep their types and names; `top_merchants` / `top_spending_merchants`, `days_without_records`, and `possible_duplicates` become more accurate.
