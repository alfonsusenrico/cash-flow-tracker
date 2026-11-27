## Approval

Pending. Requested by the owner on 2026-11-24 ("ok go with B, plan it") after a Kalender day showed less than the expenses listed under it.

## Why

A day's Pengeluaran is its expenses less a share of the period's reimbursements. A linked reimbursement lowers its expense's day; an unlinked one, naming only a category, is spread over every day of that category in proportion to its spending. When repayments naming Transportasi are recorded without links, a later Rp 10.000 parking counts as about Rp 7.800, and its day shows less than its rows. The rule is correct for the period, but a day can no longer be checked against the rows listed under it, and the fix (linking) is work the owner should not need for a day to read right.

The owner chose option B on 2026-11-24: days at full price, period totals still net of reimbursements.

## Expected Outcome

- Every daily figure is the full price of that day's rows: a day with a Rp 190.000 meal and Rp 10.000 of parking reads Rp 200.000, whatever was repaid.
- Every period figure stays exactly as it is: Pengeluaran, Belanja, categories, pillars, comparisons, budgets, the monthly report and Beranda.
- Where days are shown beside the period total, the gap is named: "Rp A dibelanjakan · −Rp B diganti", with A the sum of the days and A − B the total.

## What Changes

- The daily series stop subtracting reimbursements: Kalender days, heat and day panel; Ringkasan's "Pengeluaran harian" chart, table and its per-pillar and per-category columns; the analytics day-of-week heatmap; the overview trendline and burn cadence.
- The calendar and analytics responses return the full-price sum of the days and the reimbursed amount beside the net total, so the page adds nothing up.
- Kalender's hero and Ringkasan's daily chart show the "dibelanjakan · diganti" line when something was reimbursed in the period.
- The daily apportionment of offsets (`daily_offsets`, `offset_day_shares`, linked parts per expense day, added in #124) is removed as obsolete.

## Non-goals

- How much is offset in a period, in which period, or by which category and pillar (unchanged; linking still decides category and pillar).
- Treating reimbursements as income (option A, declined).
- A marker on the day a reimbursement arrived; its row already shows in that day's list.
- Linking, the reimbursement screens, the monthly report, Beranda and Transaksi.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `reimbursement-offsets`: the daily series are no longer net of reimbursements; "Offset Attribution to Spending Days" is removed.
- `cycle-calendar`: a day's spending is its full price; the days add up to the full-price total, which less the reimbursed amount is the period's figure.
- `analysis-workspace`: the daily table's pillars and categories are full price; the daily views name the reimbursed amount.

No project capability is added; `money` applies: every period total must stay identical, verified by tests on the existing seeds.

## Impact

- **Backend:** `services/spending_basis.py` (days from gross rows, totals unchanged), `services/reimbursements.py` (drop per-day linked parts and `offset_day_shares`), `services/report_figures.py` (drop `daily_offsets`), `services/calendar.py` and `routers/dashboard.py` (gross days, new total fields, add-up checks); tests.
- **Frontend:** Kalender hero line, Ringkasan daily chart line, types, `id` and `en` messages; tests.
- **API:** additive fields (`totals.spending_gross`, `totals.spending_reimbursed` on the calendar; `spending_totals` on analytics); existing daily fields keep their names and change meaning from net to full price. No migration.
