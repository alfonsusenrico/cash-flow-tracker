## 1. Fix

- [x] 1.1 Reproduce: a flagged pocket keeps its flag after a successful match. Evidence: `test_saldo_cocok_clears_the_pockets_warning` failed on base `9e5be43`.
- [x] 1.2 `record_match` clears the checked account's flag. Evidence: the tests for all three new scenarios pass.
- [x] 1.3 Full backend suite and `openspec validate --all --strict`. Evidence: `verification.md`.

Expected Result: one "Saldo cocok" removes "Perlu dicocokkan".

## 2. Deliver

- [x] 2.1 After the owner accepts, archive, push, open the PR and report its URL.
