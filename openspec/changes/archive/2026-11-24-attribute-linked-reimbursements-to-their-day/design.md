# Design: attribute-linked-reimbursements-to-their-day

## Context

`load_spending_offsets` returns three parts for a window: `linked` (linked amounts by the expense's category and pillar, for expenses dated in the window), `fallback` (unlinked reimbursements by the category they name) and `rest` (what linked reimbursements dated in the window hold on no expense). `spending_basis` then spreads each (category, pillar) offset over that group's days in proportion to their gross amounts with `apportion`, so days, categories and pillars all add up exactly. The reimbursement-offsets spec requires this ("Offset Attribution to Spending Days").

A link already belongs to one expense, is capped at what that expense cost after earlier links ("Reimbursements Linked to Expenses", "Never below zero"), and already counts in the expense's own period.

Production example (2026-11-24, read-only): two Rp 68.450 links to the 31 October Mie Tore meal (Rp 205.350, Makanan & Minuman, need) are spread over the cycle's Rp 1.172.540 of Makanan & Minuman need; 23 November's Rp 88.290 of it carries Rp 10.308, so the day reads Rp 98.982.

## Decisions

### 1. A linked amount lowers its expense's own day

For each linked part in the window, the offset is subtracted on the expense's local day, in the expense's category and effective pillar. The loader returns linked parts per (expense day, category, pillar) instead of per (category, pillar). Because a link never exceeds what its expense still costs, a day's group can never go below zero from links alone.

### 2. Unlinked offsets keep the spread

`fallback` and `rest` offsets have no expense, so they keep today's proportional spread over the group's days, applied to what is left of each day after its linked amounts. The existing floor (an offset never larger than the group's spending) is unchanged.

### 3. Totals are untouched

Only the distribution over days changes. The period total, `by_category`, the Kakeibo pillars and `reimbursed` are computed as today; tests assert they are byte-identical on the existing seeds.

Rejected: subtracting a reimbursement on the day it arrived (it would make the arrival day look cheaper and could drive it below zero).

## Risks

- A linked expense with a later edit (amount lowered below its links) is already handled by "Links Follow Changes", which re-allocates the links; the day follows.
- The daily series of a period that only holds the reimbursement (expense in the previous period) is unchanged, as today: the link counts in the expense's period.

## Verification

- Backend: the 23 November case (synthetic): a linked meal on one day and other spending in the same category on another; the other day shows its full amount and the meal's day its net; sums of days, categories and pillars equal the unchanged totals; an unlinked reimbursement still spreads; existing reimbursement and analytics tests pass with their totals unchanged.
- Browser: not needed beyond a smoke check of Kalender and Ringkasan; the change is server-side only.
