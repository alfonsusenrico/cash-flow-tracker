# Design

## Context

This change is stacked on `audit-financial-calculations-and-automation` (branch `feat/financial-reporting-consistency`, 4220bba), which is not yet archived. That change deferred the essential-cost rework ("Essential cost and coverage — deferred"), kept owner-only plan linking ("Open decisions for owner": auto-linking a single exact candidate) and left Analisis with descriptive charts and an unequal-window comparison. On 2026-10-30 the owner approved this change and reversed the deferral (see proposal.md, Approval).

Facts from the code at 4220bba:

- `calculate_ketahanan_dana` summed the last 30 days of expenses in categories with `COALESCE(is_primary, TRUE)`, fell back to primary budgets, added debt minimums, and returned `runway_days = 999`, `runway_months = 99.9` and status `healthy` when the cost was 0. `/dashboard/net-worth` passed total assets (including investments) as the no-reserve fallback, `/dashboard/overview` passed liquid cash, and net-worth ignored the owner's target multiplier.
- Only the Dompet page displays coverage (from `/dashboard/net-worth`); the overview fields are not shown on the web. The Android companion reads only `/dashboard/cash-flow-summary` (checked read-only in `financial-tracker-mobile-listener-financial-reporting`), so no companion change is needed.
- `is_primary` is still read as the fallback pillar for categories without `kakeibo_type` (categories and transactions routers).
- Plan occurrences live in `recurring_executions` with a partial unique index on linked transactions (V37). Links, skips and records advance `next_due_date` without checking later occurrences.
- Reimbursements are applied per category and apportioned to pillars with `apportion` (largest remainder).

## Goals / Non-Goals

**Goals:** answer problems (2), (3) and (4) with observed, explainable figures; never present an invented value; keep every money invariant (reversible debt payoff, one link per transaction, no parent/child double counting); keep old response fields readable.

**Non-Goals:** annual or irregular essential costs, Android UI, AI calls, paid services, projections. Production deployment and archiving are owner actions.

## Decisions

### 1. Essential monthly cost (D1)

- **Periods.** Up to six completed periods, newest first: closed confirmed salary cycles (receipt instant to next receipt instant), then complete calendar months that end before the first confirmed receipt. Without confirmed receipts, or when same-day receipts need review, calendar months only. The current period is excluded. A period counts only if it began on or after the local day of the owner's first recorded transaction, so months before tracking began never count as zero. *Rejected:* cycles only (one confirmed salary would leave months of history unused); including the first partial month (it understates the cost).
- **Amount per period.** Expenses whose effective pillar is `need` (transaction pillar, else category pillar, else `need`), in reportable categories, not movement legs, not on investment accounts, not linked to a debt (`obligation_id` or allocation rows). `unexpected`, `want`, `culture` and `saving` are excluded entirely. Movement fee rows stay, because they are real costs in their own category. Uncategorised expenses count as `need` under the existing effective-pillar rule.
- **Reimbursements.** Each category's applied reimbursement (from the shared offsets) is apportioned with the existing largest-remainder `apportion` over that category's expenses keyed by pillar and by whether the row counts here; only the `(need, counted)` share reduces the period. Excluded rows keep their own share, so a debt payment in a reimbursed category never absorbs the essential share. *Rejected:* reusing `applied_by_pillar` directly (it includes shares of excluded rows).
- **Statistic.** Median of the per-period amounts; an even count averages the middle pair and rounds half up to whole rupiah. *Rejected:* mean (one-off months move it), trimmed mean (needs more periods than most owners have), the old 30-day sum.
- **Debt.** The sum of `LEAST(minimum_payment, remaining_amount)` over active debts is added once. Linked debt payments are excluded from periods, so nothing is counted twice. Capping at the remaining balance avoids counting a minimum larger than the final payment. *Rejected:* counting actual debt payments in periods (double count with minimums and irregular prepayments).
- **Fallbacks.** No completed period: the sum of monthly budgets of active, reportable `need` categories plus debt minimums, method `need_budget`. Without such budgets: `null` with method `unknown`. A known amount of 0 is reported as 0, but coverage is then unavailable.
- **Basis returned.** `essential_cost` carries `amount`, `living_amount`, `debt_minimum`, `method`, `basis` (`history`, `budget`, `unknown`), `period_count`, `limited_history` (fewer than three periods), `cycle_status` and `periods_used` with kind, label, UTC boundaries, local dates, days, `gross_need`, `reimbursement_offset` and `amount`.
- **`is_primary`.** No longer read by the emergency calculation. It remains a column and API field because it is the legacy pillar fallback for categories without `kakeibo_type`; the web category form no longer offers the checkbox, whose help text described the retired rule. *Rejected:* dropping the column (older clients send it, and the fallback still uses it).

### 2. Ketahanan Dana coverage (D2)

- **Balance.** Designated emergency goals as before: flagged goals, else goals named "darurat"/"emergency", with linked parent/child pockets de-duplicated. Otherwise all liquid cash with `balance_basis: "all_liquid"`. Both `/dashboard/overview` and `/dashboard/net-worth` now pass liquid cash and the owner's multiplier. *Rejected:* keeping total assets on net-worth (investments are not an emergency reserve and the two screens disagreed).
- **Coverage.** `coverage_months = round(balance / cost, 1)`, `runway_days = round(balance × 30.4 / cost)`; both `null` when the cost is unknown or 0, with `status: "unknown"` and `coverage_unavailable_reason`. A non-positive balance with a known cost is 0 months, status `zero`. Status thresholds against the target multiplier are unchanged.
- **Compatibility.** `runway_months`, `monthly_primary_expense`, `target_amount`, `coverage_pct`, `daily_burn_rate` and `monthly_commitments` keep their names and become `null` when unknown; `primary_spent_30d` is always `null` (retired basis). New fields: `coverage_months`, `balance_basis`, `essential_cost`, `coverage_unavailable_reason`. Overview KPIs add `coverage_months`, `emergency_balance_basis` and `essential_cost`.
- **UI.** Dompet shows coverage or "Belum dapat dihitung", the basis sentence (for example "Median 4 periode, kebutuhan (need) saja + cicilan minimum"), the limited-history label, the all-liquid notice "Belum ada dana darurat khusus — dihitung dari seluruh saldo kas", and a disclosure listing each period with a ledger link for its exact window and `need` rows.

### 3. Automatic plan linking (D3)

- **Rule.** A due occurrence (active plan, type `expense` or `income`, `next_due_date` ≤ local today, not resolved) is linked when exactly one transaction matches all of: same owner and type, the plan's account, identical amount, local date from due − 3 to due + 7 days and not in the future, not a movement leg or movement fee, not linked or posted for another occurrence, not rejected for this plan, and, for a debt plan, not on an investment account and not linked to another debt. Transfer and top-up plans are never auto-linked because their payments are movement legs.
- **Conservative addition.** The transaction must also not be a candidate for any other due, unresolved plan; otherwise neither is linked. *Rejected:* linking the earliest plan (a silent guess between two identical plans).
- **Effects.** The link reuses the owner link path: `link_payment_to_obligation` applies debt progress once, the V37 partial unique index refuses a second occurrence for the transaction, and the rule row lock serialises concurrent passes. The occurrence stores `link_method` (`auto` or `owner`) and `obligation_applied`; `recurring_link_events` records rule, date, transaction, action and method only.
- **Undo.** `POST /recurring/{id}/occurrences/unlink` (owner) deletes the occurrence row, reverses the allocation only if this link applied it (exactly once, because the allocation row is deleted under lock), records the pair in `recurring_link_rejections`, moves `next_due_date` back to the occurrence if it is earlier, and logs an `unlink` event. A repeat returns `changed: false`. The pair is never auto-linked again; the owner may still link it by hand. *Rejected:* a new `unlinked` status (every consumer would need to learn it; the event table keeps the history).
- **Advancing.** Resolving an occurrence now advances to the first later occurrence that is not already resolved, so an undone link cannot leave a plan pointing at a met occurrence.
- **Triggers.** `request_plan_linking(owner)` only adds the owner to an in-memory set and wakes a background task; it runs after commit in transaction create and update, notification acceptance, worker claims, manual resolution, text capture, and plan create and update. The existing periodic loop (salary detection, every minute) also sweeps owners with due plans in pages of 20. Neither path adds database work to the recording request. `PLAN_AUTO_LINK_ENABLED=false` is a kill switch; tests set it so background passes never race assertions.
- **UI.** The due-plan panel lists links made automatically in the last 7 days as "Ditautkan otomatis" with "Batalkan tautan". Unlinked due plans keep their candidates and manual actions.

### 4. Observed Analisis insights (D4)

- **Fair comparison** (`GET /dashboard/period-comparison`). The selected cycle or month against the previous one, both measured from their starts over `min(elapsed current, length previous)`. When the previous period is shorter, the current window is shortened too and `current_truncated` says so. Returns inflow, outflow, net, five pillars (net of reimbursements), and the top three category increases and decreases (net) and merchant increases and decreases (gross; reimbursements apply to categories). When the previous window has no transactions, `previous_has_data: false` and changes are `null`. No percentages are shown.
- **Cash flow history** (`GET /dashboard/cash-flow-history`). The same period rule as D1 (`timeframe=month` forces calendar months): six completed periods plus the current one labelled `in_progress` ("berjalan"), each with inflow, net outflow, net and window.
- **Recurring charges** (`GET /dashboard/recurring-charges`). Expenses from the first day of the month six months back, excluding movement legs and fees, investment accounts and "Internal Movement"/"Investasi". Grouped by a merchant key (merchant, else notes; NFKC casefold, digits and punctuation removed). A group qualifies with at least 3 occurrences, a dominant interval holding at least two thirds of the gaps in the weekly (6–8 days) or monthly (25–35 days) band, and an amount coefficient of variation ≤ 0.15. A charge not seen for more than 1.5 intervals is treated as ended. Typical amount: median of the last three. Next date: last date plus one calendar month or seven days, flagged as an estimate. Monthly equivalent: weekly × 52/12. A group is "sudah ada rencana" when one of its transactions is linked to an active expense plan, the plan name has the same key, or the plan has the same account and cadence and an amount within 15%. "Jadikan rencana" opens the plan form prefilled from `plan_draft`; saving uses `POST /recurring`, which cannot post (V37 constraint).
- **Drill-down.** The ledger accepts `from`, `to` (ISO instants) and `q` in its URL and sends them as `from_date`, `to_date` and `q`, so each insight links to its exact window, category, pillar or merchant.
- **Replaced UI.** The comparison tab's four unequal-window delta cards and its narrative card are replaced by the fair comparison; the API fields remain for compatibility and the home page keeps its narrative.

## Research

- Recurring detection follows vendor practice: normalise the merchant, look for a dominant interval with low amount variance, and require a minimum number of occurrences — https://docs.spade.com/reference/recurring-transaction-guide and https://subtracker.io/build/recurring-charge-detection.
- Essential expenses and reviewing several months of spending: https://www.consumerfinance.gov/an-essential-guide-to-building-an-emergency-fund/ and https://www.consumerfinance.gov/owning-a-home/prepare/assess-your-spending/.
- The thresholds (six periods, median, three occurrences, 6–8 and 25–35 day bands, two-thirds dominance, CV ≤ 0.15, 1.5-interval end, −3/+7 day link window, 15% plan tolerance) are implementation choices informed by these sources, not values the sources prescribe. None of this is financial advice.

## Archive order

This change MODIFIES requirements that the base change adds or modifies (`clean-core-ledger` "Time-Series & Multi-Angle Dashboard Aggregations", `reimbursement-offsets` "Reimbursements in Cycle Spending and Safe-to-Spend", `obligations-debt-tracker` "Linking Ledger Payments to Obligations"); those deltas are written from the base change's versions with every scenario name kept. It MUST be archived after `audit-financial-calculations-and-automation`.

## Risks / Trade-offs

- [Incomplete capture lowers a period's need spending] → the median needs a majority of low months to move; `limited_history` and the period drill-down make thin history visible.
- [A missed salary receipt makes one cycle twice as long] → the median limits its effect; each period shows its days. Normalising by length is a possible follow-up.
- [Uncategorised spending counts as need] → follows the shared effective-pillar rule; categorising it moves it out.
- [An auto-link is wrong] → only exact, single, unclaimed matches link; the owner sees and can undo it, which reverses debt progress once and remembers the rejection.
- [Editing an auto-linked payment] → the link stays, as with owner links; debt reversal on edit uses the existing allocation path.
- [A second identical payment arrives after an automatic link] → the check runs when payments are recorded, so the first exact payment is linked and a later twin does not reopen it (seen while seeding the browser check); the label and undo let the owner correct it.
- [In-memory requests are lost on restart] → the minute sweep catches every due plan.
- [Recurring detection misses a changed merchant text or a price change over 15% variation] → thresholds are documented; the owner can still create plans by hand.

## Migration Plan

1. Deploy the backend through the approved pipeline; Flyway applies V38 (additive columns, two tables, one index, existing links marked `owner`). V38 is repeatable.
2. Check after deploy: `SELECT COUNT(*) FROM recurring_executions WHERE status = 'linked' AND link_method IS NULL` is 0; due plans with one exact payment show "Ditautkan otomatis" within a minute.
3. Rollback: redeploy the previous release. The new columns and tables are ignored by older code; occurrences linked automatically stay linked (valid links). To stop auto-linking without a rollback, set `PLAN_AUTO_LINK_ENABLED=false`.

## Open decisions for owner

- **Irregular essential costs** (annual insurance, school fees, vehicle tax) are not in the essential cost; a later change could add sinking-fund plans.
- **Long cycles** from a missed salary receipt are not normalised to a month.
- **Recurring-charge thresholds** are implementation choices; adjust after a few months of real use.
