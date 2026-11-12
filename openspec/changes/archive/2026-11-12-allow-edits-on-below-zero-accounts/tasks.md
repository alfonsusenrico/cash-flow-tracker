## 1. Reproduce

- [x] 1.1 Regression tests in `backend/tests/test_edit_below_zero_balance.py` for every scenario in the spec delta. Evidence: on `origin/main` `4597f91` three fail with 409 and the owner's exact message (`Tersedia Rp 0 dari Rp 189.667`), and the two refusal cases pass.

Expected Result: the owner's failure is reproduced by a test.

## 2. Fix

- [x] 2.1 `update_transaction` checks funds only when the edit takes extra money from a liquid account. Evidence: the four transaction tests pass.
- [x] 2.2 `update_movement` applies the same rule to its source account. Evidence: the movement test passes.
- [x] 2.3 Full backend suite, ruff and `openspec validate --all --strict`. Evidence: results recorded in `verification.md`. Done: 2260 passed; no backend linter in CI.

Expected Result: edits that take no extra money save on any balance, and raises are still refused.

## 3. Deliver

- [x] 3.1 After the owner accepts, archive the change, push `fix/edit-keeps-balance-check`, open the PR and report its URL.
