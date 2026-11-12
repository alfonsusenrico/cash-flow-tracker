# Verification: allow-edits-on-below-zero-accounts

Date: 2026-11-12 · Branch `fix/edit-keeps-balance-check` · Base `origin/main` `4597f91` · Code under test `825c9e2`.
Plan approved by the owner on 2026-11-12 ("approved, build it").

## Reproduction

`backend/tests/test_edit_below_zero_balance.py` on `4597f91` (code unchanged): 3 failed, 2 passed. The pillar-and-note
edit returned HTTP 409 with "Saldo tidak mencukupi. Tersedia Rp 0 dari Rp 189.667 yang dibutuhkan.", the owner's exact
message. Lowering the amount and editing a movement failed the same way. The two refusal cases passed.

## What changed

- `update_transaction`: the funds check runs only when the new amount exceeds what the row already took from the same
  account (the whole amount on another account or after a change to expense). When it runs, the test and message are the
  same as before.
- `update_movement`: the same rule for the source account, using the old outbound leg's account and amount.

## Checks

| Check | Result |
| --- | --- |
| Regression tests (5) | passed on `825c9e2` |
| Full backend suite, as CI (`python -m pytest tests/ -ra`, Postgres 16, Flyway V1→V54) | 2260 passed |
| `git diff --check` | clean |
| `openspec validate --all --strict` | 85 passed, 0 failed |

Data: synthetic accounts and amounts in a local test database.

## Not verified

- Production: not checked which record took the owner's Main Pocket below zero. The owner's edit is confirmed after
  deploy by the owner.
- No backend linter runs in CI, so none was run. No frontend change.
