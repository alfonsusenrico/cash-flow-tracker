## Context

See `proposal.md`. State on the base (efcf3ef, phase 3 archived):

- `notification_application.record_observed_leg` asks `notification_context.discover_candidates` for
  rows recorded from notifications with the **same amount** within ±`NOTIFICATION_PAIRING_WINDOW_SECONDS`
  (900 s, open interval), keeps those `signatures_compatible` with the new leg, links a unique one
  (after a reverse check that the counterpart has no competing leg) through
  `movement_linkage.link_transactions_as_movement`, and otherwise records the leg alone; ambiguity only
  sets `error_code` and the new leg's `ambiguous_movement` reason.
- `signatures_compatible` accepts (a) both legs with transfer wording plus a shared reference, or a
  named remote account plus owner identity; or (b) any two legs on different root accounts when
  neither names a third party.
- A self-transfer (transfer wording plus owner identity) without a counterpart is recorded in
  "Internal Movement" with `review_status = auto`. Reports exclude that category by name and flag, so
  it is not spending, but the other account never moves.
- `POST /movements/{id}/split` unlinks both legs and restores notification categories; nothing records
  that the owner split them. Manual rows are never candidates; no later pass exists.
- Lifespan background tasks: price sync, recurring scheduler (`asyncio.to_thread` + sleep loop),
  notification worker. Notification processing serialises per owner with
  `pg_advisory_xact_lock(hashtextextended('notification:<user>', 0))`.

## Goals / Non-Goals

**Goals:** fewer half-recorded transfers without ever linking unrelated rows; one rule set for every
pairing path; the money invariant (a movement is two linked legs of equal amount) intact.

**Non-Goals:** see the proposal.

## Decisions

### D1. One pairing module

`services/movement_pairing.py` owns candidate selection, the fee rule, the evidence rule, the
uniqueness checks, ambiguity flags, linking with a fee, the deferred pass and owner completion.
`record_observed_leg` keeps role confirmation of complete movements (`discover_candidates`, exact
amount, linked rows included) and delegates the pairing decision to `find_counterpart` with the
immediate window. `discover_candidates` still feeds the model context unchanged.

*Rejected:* extending `discover_candidates`. It also serves role confirmation and model context, which
need linked rows and exact amounts; mixing a fee range and manual rows into it would change both.

### D2. Candidates

An unlinked `expense` or `income` row with `source` notification or manual, on an active liquid
account without instrument, with no goal, obligation, recurring rule, debt allocation, fee link or
`owner_split_at`, and either:

- recorded by an `observed_leg` notification event and never reclassified (`owner_edited_at IS NULL`); or
- filed in "Internal Movement" (by the pipeline as a self-transfer, or by the owner, manual rows
  included).

Evidence per row: the stored event signature, with account, direction and roots recomputed from the
current accounts (the owner may have moved the row or the account). A row in "Internal Movement"
carries `transfer`, `self_identity` and no third party: filing it there is the owner's or the
pipeline's statement that it is half of an own transfer. Time is `transactions.date`.

*Rejected:* every manual row as a candidate. Manual rows have no wording; any purchase of the same
amount would then pair with a top-up. *Rejected:* keeping owner-reclassified notification rows. A
deferred pass hours later would overwrite the owner's category with "Internal Movement".

### D3. Fee tolerance

`transfer_fee(outbound, inbound)` is 0 for equal amounts, the difference when it is in
`MOVEMENT_TRANSFER_FEES` (default `2500,6500,1000,2900`) and at most `MOVEMENT_FEE_MAX_DIFFERENCE`
(default 10000), else None. Only an outbound leg larger than its inbound leg is a fee. A fee needs
strong evidence (D4). The candidate query fetches opposite legs within `[A − max, A]` for an outbound
leg of A and `[B, B + max]` for an inbound leg of B, then applies the table.

*Rejected:* any difference up to the maximum. A Rp 4.000 difference has no known cause and would hide
real mismatches.

### D4. Evidence strength and uniqueness

`pairing_evidence(left, right)` returns `strong`, `weak` or None; `signatures_compatible` keeps its
meaning (`is not None`). Rule (a) is strong. Rule (b) is strong when either leg names the owner
(`self_identity`) or names the other leg's institution (`remote_root_account_id` equals the other
leg's root; `transfer_signature` now stores it), and weak otherwise.

A pair is accepted when:
1. the counterpart is the leg's only match (fee, gap and evidence rules), and the leg is the
   counterpart's only match among the counterpart's own candidates (both directions unique);
2. a weak pair has fee 0, a gap below `NOTIFICATION_PAIRING_WINDOW_SECONDS`, and no other candidate
   of that exact amount (either direction) within that window around both legs.

Otherwise no link. Ambiguity from (1) flags the leg and every competing match; ambiguity from (2) flags
the leg and the would-be counterpart only, because the other same-amount rows prove nothing about a
transfer. More than 50 candidates is treated as ambiguity, as before.

Flagging (`flag_ambiguous`) sets `review_status = needs_review` only on unlinked rows that are `auto`,
and appends `ambiguous_movement` once to the event's `interpretation.review_reasons`. Reviewed rows are
never reopened, so a repeated pass cannot undo the owner's review.

*Rejected:* requiring both legs to be self-identified for every cross-institution pair. The real
myBCA "at Shopping" debit plus ShopeePay top-up (no names on either side) would then never pair; a
unique amount within 15 minutes is good evidence for that wording.

### D5. Linking with a fee and the fee row

`link_pair(expense, income, fee)` locks both rows in id order, re-checks that both are unlinked and
that the fee rule still gives `fee`, reduces the outbound leg by the fee, calls
`link_transactions_as_movement` (which still requires equal amounts), then inserts the fee row:

| Field | Value |
|---|---|
| account, date | the outbound leg's |
| category | owner's "Biaya Admin & Transfer" (created on first use: expense, icon `receipt`, `#64748b`, pillar need, not excluded) |
| kakeibo_type | the category's pillar, else need |
| notes | "Biaya transfer" |
| idempotency_key | `movement-fee:<movement_id>` |
| fee_for_movement_id | the movement id |
| provenance | movement, auto, system |

Then both legs go from `needs_review` to `auto` unless owner-classified (phase 3 D1). The source
account's total debits are unchanged (leg + fee = original debit).

Link mechanism: a nullable `transactions.fee_for_movement_id` with a unique partial index per owner.

*Rejected:*
- The fee row sharing `movement_id`. Every movement reader (split, delete, ledger consolidation, top-up
  guard) assumes exactly two rows per movement id; a third row breaks the invariant.
- Only the idempotency key as the link. It works for uniqueness but makes traceability a string
  convention that reports and phase 6 UI would have to parse.
- A foreign key. A movement id has no row of its own.
- Keeping the outbound leg at the debited amount. The legs would differ, which the invariant forbids.

No check constraint ties `fee_for_movement_id` to the row's type or link: the owner may edit or
convert a fee row like any expense, and a constraint would turn those edits into server errors.
`link_transactions_as_movement` rejects fee rows with 409 instead.

### D6. Undoing the fee

`POST /movements/{id}/split` first calls `undo_movement_fee`: it deletes the movement's fee row while
it is still an unlinked expense and adds its current amount back to the outbound leg, so the split
legs show what each account moved. The notification results rebuilt by split use the restored amount.
`DELETE /movements/{id}` deletes the fee row with the legs. `PATCH /movements/{id}` leaves the fee row
alone (it is the owner's edit of the movement only).

### D7. Owner splits

Split sets `owner_split_at = NOW()` on both legs. Rows with it are never pairing candidates, in the
immediate path or the pass. Manual merge still works.

*Rejected:* a per-pair exclusion table. A split usually means the automatic match was wrong; letting
the pass pair either leg with another row of the same amount would most likely be wrong again.

### D8. Deferred re-pairing

`repair_unpaired_movements()` lists owners with unlinked candidate rows created within
`MOVEMENT_REPAIR_LOOKBACK_HOURS` (default 48). Per owner, in one transaction under the per-owner
notification advisory lock: load up to 500 such legs ordered by date, run `find_counterpart` with gap
`MOVEMENT_REPAIR_MAX_GAP_SECONDS` (default 21600; strong evidence only beyond the immediate window,
D4), link each decision inside a savepoint (`pair_legs`), and flag ambiguity. A savepoint failure
(409 because a row changed) is counted as `skipped` and the pass continues.

`pair_legs` renames both legs "Pindah saldo ke <target>" when at least one leg came from a
notification (the owner-merge rule) and updates the legs' `observed_leg` events: `movement_id`, own
`confirmed_role`, a movement `result_snapshot` with the earliest event's `result_key` (both results
then share one record key, as immediate pairing does), and closes an open purpose question.

The lifespan task `_movement_repair_loop` runs the pass every
`MOVEMENT_REPAIR_INTERVAL_SECONDS` (default 600) in a worker thread, logging only the counts
(`paired`, `with_fee`, `ambiguous`, `skipped`) and exception class names. The first run waits one
interval after start: legs recorded just before a restart were already tried immediately, and
short-lived processes such as the test client never run it.

Concurrency: two passes, or a pass and notification processing, serialise on the advisory lock and
re-read candidates under it, so a linked row is no longer a candidate. A concurrent owner merge locks
the same transaction rows; `link_pair` re-checks under the row locks and the loser gets 409.

*Rejected:* a separate scheduler table or queue. The pass is cheap (an indexed scan of recent unlinked
rows) and idempotent; a queue adds state with no benefit at personal scale.

### D9. Awaiting counterpart

A leg recorded without counterpart in "Internal Movement" gets the reason `awaiting_counterpart`, so
an automatic row becomes `needs_review`. It keeps "Internal Movement" (not spending or income) and no
purpose question, and stays a candidate (D2).

`POST /api/movements/complete`:

```json
{"transaction_id": "<uuid>", "target_account_id": "<uuid>"}   // outgoing leg
{"transaction_id": "<uuid>", "source_account_id": "<uuid>"}   // incoming leg
```

Order, under the per-owner notification lock and a row lock on the transaction:
1. 422 (body) unless exactly one account id; 404 unknown or foreign transaction.
2. 422 `counterpart_side_mismatch` when the side does not match the row's type.
3. Linked row: when its partner was created by this completion (`idempotency_key =
   movement-complete:<transaction_id>`) on the same account → 200 `idempotent: true`; else 409
   `already_linked`.
4. 422 `not_awaiting_counterpart` unless the row is in "Internal Movement"; 409
   `transaction_not_completable` for goal, debt, recurring or fee rows.
5. 422 `same_account`; 404 for an unknown, foreign or archived account (`lock_owned_accounts`); 422
   `investment_trade_required` for an investment position; 422 `account_not_liquid`; 422
   `account_has_pockets` for a parent with pockets.
6. Insert the missing leg (same amount and date, "Internal Movement" of its kind, notes "Pindah saldo
   ke <target>", key `movement-complete:<transaction_id>`, provenance movement/reviewed/system), link
   both, set both `review_status = reviewed`, and update the notification result (D8).

Response: `{"ok", "movement_id", "expense_transaction_id", "income_transaction_id",
"created_transaction_id", "idempotent"}`.

Completion does not check funds: the observed half already happened at the bank, like notification
movements (`allow_negative`). It is a review action, not a classification: no owner-edit fields and no
classification feedback are written.

*Rejected:* resolving a parent account to its default pocket like `POST /movements`. That helper may
write the parent's default pocket as a side effect; phase 6 UI will list pockets explicitly.

### D10. Settings

| Variable | Default | Valid |
|---|---|---|
| `MOVEMENT_TRANSFER_FEES` | `2500,6500,1000,2900` | integers, each 1…max difference |
| `MOVEMENT_FEE_MAX_DIFFERENCE` | 10000 | 0–100000 |
| `MOVEMENT_REPAIR_INTERVAL_SECONDS` | 600 | 60–86400 |
| `MOVEMENT_REPAIR_LOOKBACK_HOURS` | 48 | 1–168 |
| `MOVEMENT_REPAIR_MAX_GAP_SECONDS` | 21600 | 900–172800 |

Any invalid value sets `movement_pairing_configuration_error = movement_pairing_configuration_invalid`,
empties the fee table (exact pairing continues), and the lifespan logs a warning and does not start the
pass. This matches the existing notification settings (an explicit error code, not a silent default).

### D11. API additions

Ledger list and detail items gain `fee_for_movement_id` (null for other rows). Notification results
of a fee pair report the movement (received) amount. Merge of a fee row returns 409.

### D12. V30 and the startup mirror

`V30__movement_pairing_fees_and_splits.sql`: `ADD COLUMN IF NOT EXISTS owner_split_at TIMESTAMPTZ`,
`ADD COLUMN IF NOT EXISTS fee_for_movement_id UUID`, `CREATE UNIQUE INDEX IF NOT EXISTS
uq_transactions_fee_for_movement (user_id, fee_for_movement_id) WHERE fee_for_movement_id IS NOT
NULL`, `CREATE INDEX IF NOT EXISTS idx_transactions_unpaired_by_date (user_id, date) WHERE movement_id
IS NULL AND owner_split_at IS NULL`. No backfill. The identical mirror
`backend/app/db/movement_pairing_fees_and_splits.sql`, run last by `init_db.py`, is a no-op after
Flyway.

## Risks / Trade-offs

- [A wrong automatic link after hours] → Only strong evidence links beyond 15 minutes, both directions
  must be unique, and the owner's split is permanent (D7).
- [More `needs_review` rows] → Intended: awaiting and ambiguous rows are honest; phase 6 adds the
  completion UI and bulk review.
- [Fee category deleted by the owner] → It is created again on the next fee; an archived one is
  reused as is.
- [Pass cost] → One indexed query per owner with recent unlinked rows, at most 500 legs, every 10
  minutes.
- [Rollback] → The previous release ignores both columns. Fee rows stay ordinary expenses and their
  movements stay valid two-leg movements; split in the old release would not return the fee (the
  owner deletes the fee row by hand).

## Migration Plan

1. Merge; the GitHub workflow deploys; Flyway applies V30; the mirror is a no-op.
2. Post-deploy checks (owner authority): both columns and indexes exist; the log shows "Movement
   re-pairing" counts only when something changed; a fee transfer produces one movement and one
   "Biaya Admin & Transfer" row.
3. Rollback: redeploy the previous release. To remove V30 entirely: drop both indexes and both
   columns (fee rows remain as ordinary expenses).

## Open decisions for owner

1. **Fee row link is a new column** `fee_for_movement_id` (D5), not a shared movement id or a key
   convention.
2. **A split leg is never auto-paired again with any row** (D7), not only with its former partner.
3. **Manual candidates are limited to "Internal Movement" rows**, and notification rows the owner
   reclassified are no longer candidates, also in immediate pairing (D2).
4. **Weak cross-institution evidence** links only exact amounts within 15 minutes and only when the
   amount is unique; ambiguity flags only the two legs involved (D4).
5. **Deferred gap defaults to 6 hours** for strongly evidenced legs (D8); the task gave only the
   48-hour lookback.
6. **Fee rows are `auto`**, not `reviewed` like owner-configured system rows (D5).
7. **Completion skips the funds check, rejects parent accounts with pockets, requires "Internal
   Movement", and marks both legs reviewed without an owner classification or feedback row** (D9).
8. **Notification results of a fee pair show the received amount**, not the debited amount (D11).
9. **No backfill**: historical one-sided rows and earlier splits are not marked (proposal non-goal).
10. **Invalid pairing settings disable fee tolerance and the pass** instead of falling back to
    defaults (D10).
11. **The strict evidence rule is unchanged**: a leg naming a third party (for example "dikirim ke BCA
    Mira Langit") can still pair strictly with an owner-named credit on that bank. Follow-up: decide
    whether a third-party name should veto the strict rule.
