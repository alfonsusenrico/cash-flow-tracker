# Design: fix-monthly-report-accuracy

## Context

See `proposal.md` ("Why"). The monthly report (`/laporan`) combines `backend/app/services/monthly_report.py`, `backend/app/services/observed_insights.py`, `backend/app/services/spending_basis.py`, `backend/app/services/report_figures.py`, `backend/app/services/capture_normalization.py`, and the frontend under `frontend/src/app/laporan/` and `frontend/src/components/report/`.

## Goals / Non-Goals

- **Goals:** Align `/laporan`'s default basis with the readiness badge; make "Tempat belanja terbesar" cover named expenses net of linked reimbursements with working search links; prevent capture amounts from leaking into merchant names; stop false empty-day flags on closed salary-cycle boundaries; fix the past-reports partial status label; eliminate obvious false positives in duplicate detection.
- **Non-Goals:** Changing period totals, unlinked category reimbursement rules, daily chart/calendar full-price behavior (`show-daily-spending-at-full-price`), or notification ingestion (`fix-notification-mapping-and-stockbit-noise`).

## Decisions

### 1. `/laporan` web default basis follows `latest_readiness`

When `/laporan` has an explicit `?basis=cycle` or `?basis=month`, that value wins immediately. When `basis` is absent from the URL, `ReportView` in `frontend/src/app/laporan/page.tsx` reads `queryKeys.reports.latest` (`GET /api/reports/monthly/latest`, already cached by `AppLayout`) and uses its `basis` (`"cycle"` when confirmed salary cycles exist, else `"month"`), showing the report skeleton while readiness is pending so the page never flashes the calendar month before switching.

**Rejected:** Changing `GET /api/reports/monthly`'s default query parameter from `month` to `cycle`. The `monthly-report` API spec explicitly defines `basis=month` as the API default; fixing the web page default matches the badge without breaking API consumers.

### 2. Top spending places fall back to `name`, subtract linked reimbursements, and clean search text

In `backend/app/services/observed_insights.py`:
- `merchant_spending()` drops `AND t.merchant IS NOT NULL`, reads `t.merchant, t.name`, and joins/aggregates `reimbursement_links` per `t.id` (`COALESCE(SUM(l.amount), 0)`). Each row contributes `max(0, t.amount - linked_reimbursed)`. Rows with `0` remaining are skipped.
- Label selection strips Unicode format (`Cf`, such as zero-width spaces) characters, uses trimmed `merchant` when present, otherwise trimmed `name`, skipping empty strings and generic system placeholders (`PLACEHOLDER_DESCRIPTIONS`).
- `merchant_key()` strips `Cf` characters before NFKC folding and punctuation/digit normalization.
- `search_text()` strips `Cf` characters, strips numeric tokens (`\d+(?:[.,]\d+)*`) rather than leaving orphaned dots/commas, collapses whitespace, and strips leading/trailing separator punctuation (`" -#*/:.,"`), falling back to the cleaned original name if nothing remains. Thus `"Warung Contoh 125.000"` yields `"Warung Contoh"` instead of `"Warung Contoh ."`.

**Rejected:** Subtracting unlinked category-only reimbursements from merchants (cannot be attributed to one merchant without distorting unrelated places); keeping `t.merchant IS NOT NULL` (hides manual and credit-account expenses whose description lives in `name`).

### 3. Strip the capture's parsed money token from model-proposed `merchant`

In `backend/app/services/capture_normalization.py`, `validate_capture_normalization()` inspects the parsed money spans of `context["text"]` (from `capture_parsing.parse_amount`). If a model-proposed `merchant` (or `description` tail) embeds that exact money token (e.g. `"Warung Contoh 125.000"` when `125.000` is the parsed amount span), strip the money token and clean surrounding whitespace/punctuation.

**Rejected:** Prompt-only change without deterministic validation (violates the project's deterministic-code boundary).

### 4. Exclude partial closing day of a closed salary cycle from `days_without_records`

In `backend/app/services/monthly_report.py` `_data_quality()`: when `period.complete` is True and `period.last_instant.astimezone(_zone()).time() < time(23, 59, 59)`, end the empty-day check at `max(local_start(period), local_end(period) - timedelta(days=1))`. Calendar months end at `23:59:59.999999` local time and are unaffected.

**Rejected:** Changing `ObservedPeriod.end_date` (salary-cycle labels deliberately read payday-to-payday per owner decision 2026-11-16).

### 5. Past-reports rail distinguishes partial closed periods from complete ones

In `frontend/src/components/report/ReportContext.tsx`, `PastReports` renders:
- `t("running")` (`"Berjalan"` / `"In progress"`) when `!item.complete`
- `t("partial")` (`"Belum lengkap"` / `"Partial"`) when `item.complete && item.tracking_started_inside`
- `t("complete")` (`"Lengkap"` / `"Complete"`) otherwise

Both `frontend/src/messages/id.json` and `en.json` receive `report.context.partial`.

### 6. Duplicate detection checks label and category non-contradiction

In `backend/app/services/report_figures.py` `possible_duplicate_sql()`, keep the same-owner, same-account, same-amount, same-type, non-movement, and `±3 minutes` window, and add:
- `(t.category_id IS NULL OR dup.category_id IS NULL OR t.category_id = dup.category_id)`
- Either row has an empty label `COALESCE(NULLIF(BTRIM(merchant), ''), NULLIF(BTRIM(name), ''))`, or both labels match case-insensitively after collapsing whitespace.

This eliminates false positives for equal-amount split repayments from different people or distinct purchases within 3 minutes while keeping true duplicates (same merchant/name or unlabeled duplicates) flagged in both `data_quality.possible_duplicates` and `GET /api/transactions?possible_duplicates=true`.

## Failure Behavior, Compatibility, and Rollback

- **API compatibility:** Zero breaking schema or endpoint changes; every field keeps its name and type.
- **Data & migration:** No Flyway migration and no stored data mutation.
- **Rollback:** Reverting the deployment restores prior behavior cleanly.

## Risks / Trade-offs

- [Named expenses with non-unique phrasing (e.g. "Makan siang") may group together in Top Places] → Acceptable and matches `recurring_charges`; every row still links directly to matching ledger rows via `q`.
- [Two true duplicate notifications where one was manually renamed before checking duplicates will no longer flag] → Desired, since an already-edited distinct label indicates intentional separation; unlabeled or identical-label duplicates remain flagged.
