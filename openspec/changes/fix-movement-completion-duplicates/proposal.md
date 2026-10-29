## Approval

The owner reported the defect on 2026-10-29. This is a corrective change under the owner's 2026-10-28 instruction to continue until the plan is done.

## Why

After phases 2, 5 and 6 shipped, Tinjau listed eight September and October transfers under "Pindah saldo menunggu pasangan". The owner had already handled these one-sided "Internal Movement" rows by hand. Two causes:

- **Queue selection.** The queue showed every unlinked "Internal Movement" row whose review status is `auto` or `needs_review`. The phase 2 backfill set never-edited notification rows to `auto`, so these older rows appeared even though no pairing rule had flagged them.
- **Duplicates on completion.** `POST /api/movements/complete` always inserts a new leg on the chosen account. When that account already holds the other leg as its own row, completing records the money twice and double-counts the balance.

## Expected Outcome

- Tinjau lists only transfers that the pairing flagged `awaiting_counterpart`. Older one-sided rows appear as ordinary rows to accept.
- Completing never creates a second copy of an existing leg:
  - one clean match is linked;
  - other matches are shown for the owner to choose from, with an explicit "Tetap buat baru".

## What Changes

- The review queue's awaiting section requires the stored reason `awaiting_counterpart`.
- `complete_movement` looks for unlinked opposite rows of the same amount within ±3 days on the chosen account.
  - **One Internal Movement match** that the owner has not split is linked to the transfer.
  - **Any other match** returns 409 `counterpart_candidates`, unless `create_new: true` is sent.
  - **Repeating a linked completion** is idempotent.
- Tinjau shows the candidates with "Hubungkan" (existing merge endpoint) and "Tetap buat baru".

## Non-goals

- Correcting production rows created by an earlier completion. That is a data correction, which the owner performs in the ledger, guided by the findings in design.md.
- Fee-tolerant matching for completion. It stays exact-amount, and the owner can still choose any row.

## Capabilities

### Modified Capabilities

- `review-queue`: the awaiting section's selection, and the candidate choice on Tinjau.
- `movement-pairing`: completion links an existing leg, or asks first.

## Impact

- **Backend:** `services/movement_pairing.py`, `routers/movements.py`, `routers/review.py`, plus tests.
- **Frontend:** `app/review/page.tsx`, `components/review/MovementReviewCards.tsx`, `lib/review.ts`, plus tests.
- **API:** additive. A new optional `create_new` field, a new 409 code, and a `linked_transaction_id` response field.
- **Database:** no migration.
