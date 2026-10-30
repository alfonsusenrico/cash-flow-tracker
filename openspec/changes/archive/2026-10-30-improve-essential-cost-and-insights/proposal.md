# Proposal

## Why

The owner reported three problems that the base change `audit-financial-calculations-and-automation` left open:

- (2) "Biaya hidup pokok bulanan" is unreliable: it is the last 30 days of spending in categories flagged primary, so one month with unexpected costs distorts it, and Ketahanan Dana then has no defensible value.
- (3) Averages and projections are not useful; the owner wants observed, actionable information instead of calculations.
- (4) Plans must not create duplicate work: a captured payment that obviously meets a due plan still has to be linked by hand.

## Approval

Owner approved on 2026-10-30 ('do what you suggest and recommend as long as it answer the problems'), superseding the 2026-10-30 deferral of the essential-cost rework; coordinator-reviewed.

## Expected Outcome

- The owner sees an essential monthly cost that one unusual month does not move, with its basis (method, periods used, limited history) and a drill-down to the periods behind it, or an explicit "unknown" instead of an invented figure.
- Ketahanan Dana shows months of coverage only when the cost is known, says plainly when it is computed from all liquid cash instead of a designated reserve, and never shows 999 days or "> 12x".
- A due plan whose payment was captured exactly once on the same account is linked without owner action, labelled "Ditautkan otomatis", and can be undone; anything less certain stays manual with its candidates.
- Analisis shows three observed insights with periods and ledger drill-down: a fair same-elapsed-days comparison, net cash flow per period, and detected recurring charges that can become a non-posting plan.

## What Changes

- Replace the essential-cost basis: median of the per-period `need` spending (net of reimbursements; movements, budget-excluded categories, investment trades and debt-linked payments excluded) over up to six completed salary cycles or calendar months, plus active debt minimums once. Fall back to `need` category budgets, else unknown (`null`).
- Remove the 30-day window and the `COALESCE(is_primary, TRUE)` rule from the emergency calculation. `is_primary` stays only as the legacy pillar fallback for categories without a pillar; the web category form no longer offers it.
- Ketahanan Dana coverage becomes `null` when the cost is unknown; the 999/99.9 sentinels are removed. Response fields are kept for compatibility but never carry invented values. The liquid-cash fallback returns `basis: "all_liquid"`. Both dashboard callers use the same liquid fallback and the owner's target multiplier.
- Auto-link a due plan occurrence to exactly one matching unlinked transaction (same type and account, identical amount, due −3 to +7 days, not a movement), applying debt progress once, recording `link_method` and an audit event, with an owner undo that reverses debt progress once and is remembered. New migration V38.
- Analisis gains a fair period comparison (per Kakeibo pillar and top category and merchant changes), net cash flow history for six completed periods plus the current one, and detected recurring charges with a "Jadikan rencana" action that prefills a non-posting plan. The unequal-window delta cards on the comparison tab are replaced.
- The ledger accepts date-range (`from`, `to`) and search (`q`) URL filters so insights drill down to their exact rows.

## Non-goals

- Annual or irregular essential costs (insurance, school fees, vehicle tax): recorded as a follow-up.
- Android UI changes: the companion reads only `/dashboard/cash-flow-summary`, which this change does not alter.
- AI calls, new paid services, projections or forecasts.
- Production access, deployment, pushing or archiving. Verification is local on disposable PostgreSQL with synthetic data; production release stays with the owner through the approved pipeline.

## Capabilities

### New Capabilities
- `emergency-cost-estimation`: essential monthly cost from observed completed periods, its basis and unknown state, and emergency coverage that never invents a value.
- `observed-insights`: fair period comparison, net cash flow history and detected recurring charges on Analisis.

### Modified Capabilities
- `clean-core-ledger`: primary-category flagging no longer feeds living cost; Ketahanan Dana uses the essential cost and its basis; dashboard aggregations expose the new coverage (from the base change's version).
- `net-worth-analytics`: the runway no longer uses a trailing burn rate or a 999-day sentinel.
- `reimbursement-offsets`: the emergency cost is net of applied reimbursements (from the base change's version).
- `automated-transactions`: exact single-candidate auto-linking and owner undo of plan links.
- `obligations-debt-tracker`: an auto-linked obligation plan occurrence applies and reverses debt progress once (from the base change's version).
- `report-periods-and-drilldown`: ledger date-range and search filters in the URL.

Standards: money invariants (reversible debt payoff, idempotency, no parent/child double counting), schema migrations through Flyway, background-job safety, WCAG 2.2 AA for the changed UI. No new capability class is added to the project.

## Impact

- Backend: `routers/dashboard.py`, `routers/recurring.py`, `routers/transactions.py`, notification and text-capture commit points, `main.py` background loop, new services for report periods, essential cost, plan auto-linking and observed insights; migration `V38__plan_link_method.sql`; settings `PLAN_AUTO_LINK_ENABLED`.
- API: `/dashboard/net-worth` and `/dashboard/overview` runway fields may be `null` and gain `essential_cost` and `balance_basis`; new `GET /dashboard/period-comparison`, `/dashboard/cash-flow-history`, `/dashboard/recurring-charges`; new `POST /recurring/{id}/occurrences/unlink`; `/recurring/pending` gains `recent_auto_links`; occurrences gain `link_method`.
- Web: Dompet coverage and basis, due-plan panel auto-link label and undo, Analisis insights, plan form prefill, ledger URL filters, category form.
- Android: no change required (read-only check).
- Ordering: this change depends on, and must archive after, `audit-financial-calculations-and-automation`.
