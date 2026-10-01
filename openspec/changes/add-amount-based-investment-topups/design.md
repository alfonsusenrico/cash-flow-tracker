# Design

## Context

See proposal.md for the requested outcome. Source inspection on main at `2361145` established these constraints:

- `transactions.py` requires `amount = units × price_per_unit` for investment trades. `ledger_mutations.py` deliberately rejects investment positions in generic movements.
- `accounts.py` already has manual amount valuation: `last_price` is the aggregate value when units are not tracked. Cost basis falls back to opening balance or aggregate `avg_buy_price`; Update Nilai currently writes cost basis into `initial_balance`.
- Movements are paired ledger entries, consolidated by the frontend. Generic movement edit/delete does not adjust investment state, so it cannot own a contribution lifecycle.
- Recurring execution already has per-occurrence claims and savepoints. Manual execution currently keys an occurrence by execution date, rather than the selected scheduled due date.
- Dashboard investment allocations already identify liquid-to-investment movements; account aggregation feeds net worth without counting parents and children twice. The main net-worth specification describes an older liquid-only summary; this change preserves the actual separate liquid/investment/total summaries, without redefining those unrelated metrics.

## Goals / Non-Goals

**Goals:** reuse paired entries and the recurring occurrence journal, with one contribution mutation service used by manual and recurring entry. Keep all money amounts as integer rupiah and serialize financial writes.

**Non-Goals:** no unit estimation, conversion of unit portfolios, redemption flow, fees, settlement statuses, bank execution, notification matching, or automatic fund quotes. No broader repair of existing trade deletion or unrelated recurring behavior.

## Decisions

### 1. Dedicated contribution contract

Add authenticated `POST /api/investment-topups`, `GET /api/investment-topups/{id}`, `PATCH /api/investment-topups/{id}`, and `DELETE /api/investment-topups/{id}`. Create accepts source/target account IDs, positive integer amount, timezone-aware transaction time, optional notes, and a bounded idempotency key. Responses expose contribution ID, movement ID, paired transaction IDs when active, and replay/deletion status.

The target must be an owned active `mutual_fund` leaf with `units IS NULL`; an explicit units value, including zero, denotes a unit-tracked product. The source must be owned active liquid cash/bank/wallet/e-wallet without an instrument. Aggregate funding selections use existing effective-account resolution, then validate and lock the actual liquid account. Ownership, leaf eligibility, and balance are checked again under the lock. Do not silently choose a product beneath an investment parent.

PATCH changes amount, time, and notes; source/target stay fixed. Reassignment requires a reversal and new entry. Errors use stable codes: 404 for unavailable references, 422 for invalid amount/unsupported product, and 409 for insufficient funds, idempotency mismatch, or an invalid reversal. Existing trade and generic movement routes keep their eligibility checks. A generic movement factory can share paired-entry mechanics, but contribution permission must be explicit; do not misuse `is_trade=True` to bypass validation.

Alternative considered: making units optional on Beli. Rejected because it conflates a unit position mutation with cash contribution and leaves valuation and reversal behavior undefined.

### 2. Explicit amount capital with a contribution journal

Add an additive versioned migration after V22 with:

- `accounts.investment_tracking_mode` nullable, with the new `amount` mode;
- `accounts.investment_cost_basis` nullable nonnegative BIGINT, where null means unknown;
- `accounts.investment_value_estimated` boolean, default false;
- an `investment_topups` journal containing owner, ID/movement UUID, source, target, current amount/time/notes, optional recurring occurrence reference, immutable create-request fingerprint, owner-scoped unique idempotency key, and deletion state.

The journal ID is the bilateral movement ID; transaction-list joins expose additive `movement_kind = investment_topup` and `investment_topup_id`. Keep foreign keys to owned entities and prevent account removal from bypassing active contributions. Archive may remain available, but edits/deletes must validate ownership and support reversing an existing contribution to an archived target without allowing new deposits to it.

Initialize amount state lazily under account locks on first contribution or amount-mode valuation. Preserve legacy known basis using the existing precedence (positive opening balance, then aggregate average price); an entirely empty product starts at zero, while a nonempty product with no known basis remains null. Do not invent a historical basis from market value. Snapshot current value from aggregate `last_price` when supplied, including zero, otherwise existing own ledger balance. Leave `initial_balance`, `units`, and legacy average price unchanged on contribution writes.

For signed contribution delta D:

`funding ledger -= D; cost basis += D if known; estimated value += D`

An edit applies new-minus-old delta; deletion applies minus-old amount. This preserves absolute gain, although gain percentage changes as invested capital changes. Reject negative resulting known basis or value. Only an increased debit needs an additional funding-balance check. With unknown basis, gains remain null even though the contribution amount is known.

Reuse aggregate `last_price` for the adjusted amount value; mark it estimated and preserve `last_price_at` so a contribution does not masquerade as a market refresh. Update Nilai sets the absolute current value, optionally the absolute total cost basis, clears the estimate flag, and stamps valuation time. It uses the same locks and canonical amount-state fields and never changes the opening balance in amount mode. Account responses and parent/dashboard totals read canonical basis and value for this mode, including explicit zero. Display cost basis separately from ledger balance. Guard raw account edits and trade execution against switching an amount-mode product with contribution history into unit tracking; conversion requires a future explicit workflow.

Alternative considered: increasing opening balance alongside the inbound ledger leg. Rejected because it double-counts contributions and mixes valuation reconciliation with ledger history.

### 3. Atomicity, correction, and retry protection

The service locks owned accounts in stable ID order and serializes mutation of one contribution. Shared valuation, top-up, and account-edit paths must use compatible lock ordering. Commit both legs, journal, and amount state in the same database transaction. A same-key create replay compares its immutable original fingerprint and returns the current journal record without writing; a changed original request gets 409. Resolve concurrent inserts through the unique key and read the winner without a second financial mutation. Preserve complete keys or use deterministic hashes; do not inherit silent movement-key truncation.

Deletion retains a journal tombstone and its retry identifier while removing both financial legs and reversing their delta. A retry of a deleted create returns a deleted/conflict response and never recreates it. Recurring execution stays fulfilled after deletion; correction of an already confirmed month is a deliberate manual operation, not schedule reset.

Generic movement/transaction edit, delete, split, and merge must identify contribution metadata and either dispatch to the dedicated lifecycle or return 409 with its identifier. The web ledger dispatches correctly. Account deletion and broad account-field updates cannot bypass this invariant.

Contributions are capital deployment: paired transfer categories, no ordinary spending/income totals, and no ordinary spending pillar. Preserve dashboard liquid-to-investment allocation counting once. Do not classify both legs as separate savings or expenses.

### 4. Manual recurring rules keyed by scheduled occurrence

Extend recurring type validation, database constraints, web types, and form validation with `investment_topup`. It requires eligible source/target and forbids category, obligation, payroll linkage, and `auto_post=true`. Revalidate the final merged rule on update and the accounts at execution. The automatic processor explicitly excludes this type even if a malformed legacy row exists.

Extend `/api/recurring/execute` additively with `scheduled_dates`, a rule-ID-to-due-date mapping. Require it for investment-top-up confirmations. Existing `rule_ids` and optional `execution_date` remain compatible for other types. For top-ups, the scheduled date identifies the occurrence; execution date is the actual confirmed debit date. Retry of a fulfilled selected occurrence returns its result. An unfulfilled selection must match the locked rule's current due date and be due; stale or future selections fail rather than executing another month.

Pass scheduled date and actual ledger date separately to occurrence execution for this type. Use the existing unique `(recurring_rule_id, scheduled_for)` claim, derive the contribution retry key from it, and call the shared service inside the savepoint. Advance from the scheduled occurrence, not a delayed confirmation date. Partial batch failures remain visible and pending; overall HTTP success is not proof that every item succeeded.

Creating a monthly rule does not record today's debit. Top up records one actual debit; Jadwalkan bulanan separately creates a manual rule, with owner-selected day. The first version confirms the saved amount; the owner edits the rule before confirmation if a future debit amount differs. No schedule day is assumed from the conversation.

Alternative considered: a separate scheduler. Rejected because the existing occurrence claims and pending UI already implement this responsibility.

### 5. Fit the existing accounts, ledger, and recurring interface

Add Top up at each eligible mutual-fund leaf, including a standalone leaf. A compact modal shows product, preferred eligible funding account (product then parent), rupiah amount, actual transaction time, and notes, with no unit/price inputs. Provide a shortcut to the existing recurring modal prefilled with type, source, product, and amount, keeping rule creation separate from debit recording. Follow current matte-charcoal/lime controls, lavender investment labeling, and tabular amounts.

Unit-tracked products retain Beli/Jual with a concise explanation of top-up eligibility. Amount-mode products with contribution history must not present an executable unit trade. Ledger detail uses Top up Investasi, the paired accounts, and the dedicated correction endpoints. Explain that value after a contribution/correction is an estimate until Update Nilai.

Recurring forms filter source to liquid accounts and target to eligible mutual-fund products; manual confirmation is fixed for this type. The pending banner displays source → product, amount, scheduled date, and records only after the owner confirms the actual debit. Inspect result statuses, retain failed items, and invalidate accounts, ledger, dashboard, pulse/insights, and recurring queries affected by successful mutations. Reuse existing modal focus/error conventions and verify desktop/mobile behavior, keyboard, focus restoration, labels, contrast, and reflow against the UI guide.

## Risks / Trade-offs

- Amount tracking cannot describe allocated units or settlement → label adjusted value as an estimate and reconcile with Bibit through Update Nilai.
- Correction after a later valuation changes that snapshot by a cash delta → preserve gain mathematically, explain reconciliation, and reject negative results atomically.
- Existing products with units, including zero units, are ineligible → preserve their data; do not auto-convert. First-version eligibility is part of owner review.
- A bank notification or another manual entry could already represent the debit → clearly identify contribution entries; do not claim cross-channel deduplication. The owner must avoid recording the same debit twice.
- Legacy cost basis may be unknown → keep gain unavailable until actual total capital is supplied.
- Old app versions do not recognize contribution lifecycle or recurring types → deploy coherent backend/web artifacts; do not permit old generic mutation paths to corrupt new records.

## Migration Plan

After owner approval, create a topic branch from current main and implement the additive migration through the existing Flyway/startup workflow. Verify V1 through the new version and existing account/rule data on disposable local PostgreSQL. Do not backfill contributions or alter historical trades, ledger balances, or production data manually.

Deploy only through the normal owner-reviewed branch and automated pipeline. Deployment is not authorized by this planning turn. Before any records use the feature, application rollback can leave unused additive schema. Once amount-mode accounts or contribution records exist, prefer a forward fix or disable new contribution entry while preserving reads; an old application may display wrong basis or mutate pairs unsafely. Do not down-migrate or restore an old binary over active new records without a separately approved recovery plan.
