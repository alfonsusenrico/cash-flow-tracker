## Approval

Owner authorized phases 2–8 on 2026-10-28 without per-phase review; coordinator-reviewed.

## Why

Phases 2–5 made the ledger honest: uncertain rows are recorded with `review_status = needs_review`,
merchant-less payments carry an "Apa ini?" question, one-sided transfers wait for their counterpart
and ambiguous legs are flagged. None of this is reachable on the web. The owner wants to open the web
once a week or month and confirm everything quickly, but today every row has to be opened one by one
in the ledger, purpose questions can only be answered on the phone, and awaiting or ambiguous
movements have no completion UI.

The reports are also not yet trustworthy (audit `research/notes/2026-10-28-automation-quality-audit.md`,
C7 and section E):

- uncategorised spending (which phase 3 now records on purpose) and spending in archived categories
  count in total outflow but appear in no category, so the category list does not add up;
- reports only know the payday cycle, not the calendar month;
- no figure leads to the rows behind it;
- the ledger summary, the daily series, the heatmap and the burn rates still use gross spending while
  the dashboard totals are net of reimbursements (phase 2 open decision 6 and design D7);
- the ledger shows neither the review status nor which movement a transfer-fee row belongs to.

This is phase 6 of `research/notes/2026-10-28-automation-quality-plan.md`.

## Expected Outcome

- A "Tinjau" page, reachable from the navigation with a count of rows needing attention, lists in one
  place: open "Apa ini?" questions (answered with a suggestion chip or text), rows needing review and
  unreviewed automatic rows grouped by merchant with count, total and date range, one-sided transfers
  waiting for their counterpart, and ambiguous movement legs with their possible counterparts.
- The owner accepts, recategorises or renames a whole group with one action. Recategorising and
  renaming teach merchant memory and write one owner-correction example per changed row, exactly as an
  edit in the ledger does. A row opens the existing ledger edit dialog.
- Category breakdowns add up to total outflow: a "Tanpa kategori" bucket and spending in archived
  categories are listed.
- Insights, the home dashboard and the ledger can show a calendar month as well as the payday cycle.
- Every category and Kakeibo pillar figure on the insights page opens the ledger filtered to the same
  period and category or pillar; the ledger keeps its filters in the URL.
- Ledger summary outflow, the daily series, the heatmap and the burn rates subtract reimbursement offsets
  the same way the dashboard totals do, so the figures agree.
- The ledger shows each row's review status and links transfer-fee rows with their movement.

## What Changes

- Backend:
  - new `routers/review.py`: `GET /api/review/summary` (badge counts), `GET /api/review/queue`
    (questions, groups, awaiting and ambiguous sections) and `POST /api/review/bulk` (accept,
    recategorise, rename, keep separate; owner-scoped, validated, one transaction, at most 200 rows);
  - bulk recategorise and rename reuse the owner-edit hooks (`classification_change`,
    `OWNER_EDIT_ASSIGNMENTS`, `snapshot_rows`, `record_owner_corrections`);
  - new `services/report_windows.py`: cycle, 30/90-day and calendar-month windows shared by the
    dashboard and the ledger;
  - new `services/report_figures.py`: one reportable-expense predicate, the category breakdown with
    "Tanpa kategori" and archived categories, and per-day attribution of reimbursement offsets;
  - dashboard overview and analytics: `timeframe=month`, the breakdown, net daily series, heatmap and
    burn rates;
  - ledger list: `period`, `period_offset` and `uncategorized` filters, a `period` summary block,
    reimbursement-net outflow, `movement_fee` and `fee_movement` link fields.
- Frontend:
  - new `/review` page and `components/review/*`; navigation entry "Tinjau" with a count badge;
  - ledger: filters read from and written to the URL, period selector (all, payday cycle, calendar
    month) with a stepper, review-status badge, fee links, `?edit=<id>` opens the edit dialog;
  - insights and home: payday-cycle / calendar-month switch; "Tanpa kategori" and archived rows; drill-down
    links on category and Kakeibo figures.
- No migration: the existing columns (V28–V30) carry everything this phase needs.

## Non-goals

- Re-enriching history with AI suggestions (phase 7) and removing the startup mirrors (phase 8).
- Writing a feedback row for a plain "accept" (no classification changed); see design open decisions.
- Linking two legs whose amounts differ by a fee from the review page; the merge endpoint still
  requires equal amounts.
- Pulse (`/api/pulse`, `/api/pulse/insights`) daily figures and the emergency-fund runway; they stay as
  they are and are listed as a follow-up.
- Restyling: the page uses the live tokens and existing components.
- Production authority. Verification is local: a disposable PostgreSQL migrated V1→V30, the full test
  suites and a headless browser check against synthetic data. Deployment happens only through the
  GitHub workflow after the owner merges.

## Capabilities

### New Capabilities

- `review-queue`: the "Tinjau" page, its summary, queue and bulk endpoints, and the navigation badge.
- `report-periods-and-drilldown`: calendar-month windows, the complete category breakdown, drill-down
  from report figures to the ledger, ledger URL filters, review-status badges and fee links.

### Modified Capabilities

- `reimbursement-offsets`: daily series, heatmap, burn rates and the ledger summary outflow now
  subtract applied offsets (previously gross).

The project capability list is unchanged. The money, personal-data and multi-user standards apply: the
bulk endpoint checks ownership of every row, writes in one transaction, keeps movement legs intact and
logs no transaction content.

## Impact

- **Backend:** `routers/review.py` (new), `services/report_windows.py` (new),
  `services/report_figures.py` (new), `routers/dashboard.py`, `routers/transactions.py`,
  `services/reimbursements.py`, `main.py`; tests.
- **Frontend:** `app/review/page.tsx` (new), `components/review/*` (new), `lib/review.ts` (new),
  `lib/reportLinks.ts` (new), `components/ui/PeriodStepper.tsx` (new), `app/ledger/page.tsx`,
  `components/ledger/MobileLedgerFeed.tsx`, `app/insights/page.tsx`, `app/page.tsx`,
  `components/dashboard/KakeiboBenchmark.tsx`, `components/dashboard/CategoryDonutChart.tsx`,
  `components/layout/*`, `lib/queryKeys.ts`; tests.
- **API:** new `/api/review/*` routes; additive query parameters and response fields on
  `/api/transactions` and `/api/dashboard/*`. Changed figures: analytics `total_outflow` and every
  breakdown, series, heatmap and burn rate listed in design D9.
- **Data:** none. No schema change.
