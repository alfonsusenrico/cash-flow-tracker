## Why

On 2026-11-12 the owner changed the Kakeibo pillar and the note of a Rp 189.667 expense on Main Pocket and the save was refused with "Saldo tidak mencukupi. Tersedia Rp 0 dari Rp 189.667 yang dibutuhkan.". Nothing about the money changed.

A liquid account may legitimately sit below zero: a settled notification or a confirmed AI capture records a real debit even when the tracked balance is short, and flags the account for reconciliation. The edit endpoints then check the whole amount of the edited row against the balance as if it were a new expense. Every later edit of such a row is refused, even one that only renames it, and the owner cannot fix the pillar, category or note of the very row that needs review. Editing a movement whose source is below zero has the same defect. Investment top-up edits already check only the extra amount and are not affected.

## Expected Outcome

- An edit that takes no more money from a liquid account than the row already did saves, whatever that account's balance is: pillar, category, note, merchant, date, a goal or bill link, or a lower amount.
- An edit that takes more money from a liquid account is refused when the account cannot cover the extra, exactly as today, with the same message.

## What Changes

- `PATCH /api/transactions/{id}`: the sufficient-funds check runs only when the edit takes more from the liquid account than the row did before (a higher amount on the same account, another account, or a change to expense).
- `PATCH /api/movements/{id}`: the same rule for the source account.
- A regression test file for both endpoints.

## Non-goals

- No change to how new expenses, movements, plans or trades are checked.
- No change to the notification and AI-capture exception or to reconciliation flags.
- No new error code or message, and no frontend change.
- Deleting an income that leaves an account below zero stays allowed, as today.

## Capabilities

- Modified: `balance-integrity-and-settlement` (edits are checked by the extra money they take).
- No new project capability.

## Impact

- `backend/app/routers/transactions.py`, `backend/app/routers/movements.py`.
- New `backend/tests/test_edit_below_zero_balance.py`.
- No migration, no API shape change, no configuration change.
