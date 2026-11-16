## Why

On 2026-11-16 the owner confirmed Tabungan's balance (Rp 82.515) with "Cocokkan saldo" → "Tandai cocok", twice (08:16:39 and 08:16:51), but the tile kept "Perlu dicocokkan". Only "Sesuaikan saldo" clears that flag; it cleared at 08:17:33 when the owner used it with no difference. The two ways of confirming a balance behaved differently, so the owner had to do it twice.

## Expected Outcome

A successful "Saldo cocok" clears "Perlu dicocokkan" on the account or pocket that was checked, in the same database transaction. A check of an account with pockets clears only its own flag; a flagged pocket keeps its warning until that pocket is checked.

## What Changes

- `record_match` clears `reconciliation_required`, its reason and event on the checked account.
- The web dialog already refreshes the accounts list after a match, so the tile updates at once.

## Non-goals

- No change to "Sesuaikan saldo" or to how flags are raised.

## Capabilities

- Modified: `account-reconciliation`.

## Impact

- `backend/app/services/balance_check.py`, tests. No migration, no API shape change.

## Approval

Owner approved 2026-11-16 ("ok build it").
