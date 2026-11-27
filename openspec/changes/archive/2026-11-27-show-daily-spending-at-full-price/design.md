# Design: show-daily-spending-at-full-price

## Context

`spending_basis` computes Pengeluaran per (category, pillar) group, subtracts the group's applied offset from its total, and distributes that offset over the group's days with `offset_day_shares`: linked amounts on their expense's day, the rest in proportion to what each day has left (#124). `daily_offsets` in `report_figures.py` does the same for the Belanja basis (`expense` in `daily_cadence`, `spend` on the calendar, the heatmap and the overview trendline). `calendar.py` raises if the days do not add up to the net totals.

Production showed the effect on 2026-11-24 (read-only check): unlinked repayments naming Transportasi, about a fifth of the cycle's Transportasi, lowered every Transportasi day by that fifth, so a Rp 10.000 parking counted as about Rp 7.800.

Only the web app reads the daily series; the companion reads none of them.

## Goals and non-goals

Goal: every daily figure equals the sum of that day's rows; every period figure is unchanged; the difference is named where both are shown. Non-goals are in the proposal.

## Decisions

### 1. Days are full price; totals stay net

A day's Pengeluaran is the sum of that day's Pengeluaran rows (reportable expenses outside the saving pillar); its Belanja is the sum of its reportable expenses. Its `by_pillar` and `by_category` split the same rows, so they add up to it. The period's Pengeluaran, Belanja, `by_category`, pillars and `reimbursed` are computed exactly as today.

Invariant, checked in code where both appear: sum of the days = full-price total; full-price total − reimbursed = net total.

Rejected:
- Reimbursements as income (option A): declined by the owner; shared bills would count in full as the owner's own spending, and pillars and budgets would rise with them.
- Subtracting a reimbursement on the day it arrived: days would no longer match their expense rows and could go negative.
- Keeping the spread and asking the owner to link: the owner's day still disagrees with its rows until every reimbursement is linked.

### 2. The gap is named, not hidden

Calendar `totals` gain `spending_gross` and `spending_reimbursed` (and keep `spending`, `spend`, `gross_spend`, `offset_applied`); analytics gains `spending_totals: {gross, reimbursed, spending}`. When `reimbursed` > 0:

- Kalender's hero adds a line under the figure: "Rp A dibelanjakan · −Rp B diganti" (en: "Rp A spent · −Rp B reimbursed").
- Ringkasan's "Pengeluaran harian" shows the same line beside its scope line, so the bars' sum is explained next to the bars.

With nothing reimbursed, neither line appears and the days add up to the total. Amounts are masked with the rest when balances are hidden.

### 3. Remove the daily apportionment

`daily_offsets`, `offset_day_shares`, `SpendingOffsets.linked_days`, `linked_by_category_pillar_day` and the loader's per-day column only serve the attribution this change drops; they are removed with their tests. `apportion` stays: the pillar split of a category's offset and other services use it.

### 4. Existing fields keep their names

`daily_cadence[].expense/spending/by_pillar/by_category`, calendar `days[].spend/spending/heat/spending_heat`, `day_of_week_heatmap[].total_spent` and the overview `cumulative_trendline`/`burn_cadence` keep their names and become full price. Renaming would break the web app for no gain; the meaning change is recorded in the specs. The overview's last `cumulative_outflow` becomes `gross_outflow`, which with `reimbursement_offset` already in the response gives `total_outflow`.

## Failure behaviour

The calendar keeps its consistency checks, rewritten for the new invariant: a mismatch raises (500) rather than showing figures that do not add up, as today.

## Compatibility and rollback

No migration and no stored data change. Rolling back the deploy restores the old daily figures.

## Risks

- Heat thresholds move slightly (days with reimbursements get warmer); accepted, they describe the days as recorded.
- A period whose reimbursements exceed a day's spending no longer shows a near-zero day; that is the intended reading.

## Verification

- Backend: a synthetic case of the same shape (an unlinked Transportasi reimbursement and parking on another day): the day equals its rows; days sum to the full-price total; total, categories, pillars and `reimbursed` are identical to before on the existing seeds; a linked meal's day is full price; calendar and analytics days agree and carry the new totals.
- Frontend: the line appears only when something was reimbursed, with masked amounts; existing analysis tests updated.
- Browser: Kalender and Ringkasan on a production build with synthetic data at 1440 and 390 px, light and dark, axe and console clean.
