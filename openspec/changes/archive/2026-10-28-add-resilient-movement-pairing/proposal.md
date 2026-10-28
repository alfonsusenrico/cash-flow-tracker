## Approval

Owner authorized phases 2–8 on 2026-10-28 without per-phase review; coordinator-reviewed.

## Why

Phase 5 of `research/notes/2026-10-28-automation-quality-plan.md` (audit findings B4, B5 and C9).
Movement legs pair only when both were recorded from notifications, have exactly the same amount and
arrive within ±15 minutes, and nothing tries again later:

- A BI-FAST, online-transfer or e-wallet top-up fee (Rp2.500, Rp6.500, Rp1.000, Rp2.900) makes the
  outbound debit larger than the inbound credit, so the two legs never pair.
- A slow second leg, or a leg the owner typed by hand, is never paired.
- A one-sided self-transfer is filed as an "Internal Movement" expense or income and the other
  account is never credited or debited; nothing tells the owner the row is half a transfer.
- The cross-institution rule links any two equal amounts on different institutions when neither names
  a third party, which can pair unrelated rows.

In one production month 14 "Internal Movement" rows had no partner leg and the owner fixed them by
hand.

## Expected Outcome

- A transfer whose outbound leg exceeds the inbound leg by a known fee is recorded as one movement of
  the received amount plus a separate "Biaya Admin & Transfer" expense on the source account, so both
  account balances match the bank and the fee counts as spending.
- A leg whose counterpart arrives late (for example 40 minutes later), or was entered by hand as
  Internal Movement, is linked within ten minutes by a background pass.
- A self-transfer whose other leg never arrives is visibly waiting: it is flagged for review with the
  reason `awaiting_counterpart`, is not counted as spending or income, and the owner (phase 6 UI, or
  the API now) completes it by naming the other account.
- Equal amounts on two institutions are linked automatically only when the evidence is unique;
  otherwise both legs are flagged `ambiguous_movement` for the owner.
- A pair the owner splits apart is never linked automatically again, and splitting returns the fee to
  the outbound leg.

## What Changes

- Database (V30 plus its identical startup mirror): `transactions.owner_split_at` and
  `transactions.fee_for_movement_id`, a unique index (one fee per movement) and a partial index for
  unpaired rows.
- Backend:
  - one pairing rule set (`services/movement_pairing.py`) shared by immediate pairing, the deferred
    pass and owner completion: candidate eligibility (notification legs and Internal Movement rows,
    including manual ones), fee tolerance from a configurable table, evidence strength (strong or
    weak), two-way uniqueness, and ambiguity flags;
  - fee pairing reduces the outbound leg to the inbound amount and inserts the fee expense in the
    system category "Biaya Admin & Transfer" (pillar need, counted in budgets), source `movement`,
    linked by `fee_for_movement_id`; split and delete undo it;
  - background re-pairing every `MOVEMENT_REPAIR_INTERVAL_SECONDS` (default 600) over legs recorded
    within `MOVEMENT_REPAIR_LOOKBACK_HOURS` (default 48), under the per-owner notification lock;
  - one-sided self-transfers flagged `needs_review` with `awaiting_counterpart`;
  - `POST /api/movements/complete` creating the missing leg of an awaiting transfer;
  - ledger list items gain `fee_for_movement_id` (additive).
- Settings: `MOVEMENT_TRANSFER_FEES`, `MOVEMENT_FEE_MAX_DIFFERENCE`, `MOVEMENT_REPAIR_INTERVAL_SECONDS`,
  `MOVEMENT_REPAIR_LOOKBACK_HOURS`, `MOVEMENT_REPAIR_MAX_GAP_SECONDS`, wired through `.env.example`,
  `docker-compose.yml`, the runtime env materializer and the deploy workflow.

## Non-goals

- Web or companion UI for completing a movement or showing the fee (phase 6).
- Re-pairing history older than the lookback, or backfilling the 14 historical rows; the owner already
  fixed them.
- Parsing the fee amount from notification text (a second amount still holds the event, audit B6).
- Changing the strict transfer-evidence rule (shared reference, or named account plus owner identity).
- Production authority. Verification is local: a disposable PostgreSQL migrated V1→V30, the full
  backend suite, and OpenSpec validation. Deployment happens only through the GitHub workflow after
  the owner merges.

## Capabilities

### New Capabilities

- `movement-pairing`: when two recorded legs are linked automatically, the fee tolerance and fee row,
  the evidence-strength and uniqueness rules, deferred re-pairing, owner splits, and completing a
  one-sided transfer.

### Modified Capabilities

- `notification-record-and-flag`: the review reasons gain `awaiting_counterpart`, the ambiguous
  counterpart scenario also flags the competing legs, and automatic pairing also closes open purpose
  questions.
- `transaction-provenance`: an added requirement for rows created by pairing: the fee row (movement,
  auto, system), the completed leg (movement, reviewed, system), and awaiting rows as `needs_review`.

The project capability list is unchanged (database, money, background-jobs, api-consumers and
multi-user apply). Standards: money invariant (a movement is exactly two linked legs of equal amount),
migration safety with rollback, owner scoping and validation of the new endpoint, idempotent
background work, logs with counts only.

## Impact

- **Migrations:** `db/migrations/V30__movement_pairing_fees_and_splits.sql` and
  `backend/app/db/movement_pairing_fees_and_splits.sql`, run last by `init_db.py`.
- **Backend:** new `services/movement_pairing.py`; changes to `notification_application.py`,
  `notification_resolution.py`, `movement_linkage.py`, `core/config.py`, `main.py`, routers
  `movements.py` and `transactions.py`; tests.
- **API:** additive. New `POST /api/movements/complete`; ledger items gain `fee_for_movement_id`.
  Merging a fee row returns 409. Notification results of a fee pair show the received amount.
- **Data:** two nullable columns, no backfill. A new expense category "Biaya Admin & Transfer" is
  created per owner on the first fee.
- **Reports:** existing movements and rows are unchanged; future fees count as need spending.
- **Cost:** none.
