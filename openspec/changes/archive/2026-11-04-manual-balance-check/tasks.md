## 1. Backend

- [x] 1.1 Migration V48 `account_balance_matches` (design, Data and migration). Evidence: applied twice on the test database, and the table and index exist.
- [x] 1.2 `services/balance_check.py`: scope (D2), window (D3), candidates (D4), explanation (D5). Evidence: tests for an exact match, a duplicate, an unpaired leg, a held report, an unexplained difference, and a parent without double counting.
- [x] 1.3 Routes `POST /accounts/{id}/balance-check` and `POST /accounts/{id}/balance-matches` (D1, D6). Evidence: tests for a matched point limiting the next window, 409 `balance_mismatch`, 409 `balance_check_not_liquid`, 404 for another owner, and 422 for a bad balance.
- [x] 1.4 Reconcile unchanged. Evidence: the existing reconcile tests pass unchanged, and a request and response comparison against `main`.

Expected Result: the API answers checks and stores matched points.

## 2. Web

- [x] 2.1 `BalanceCheckDialog` with input, result, causes, explanation and actions (D7). It opens from the account detail. Evidence: Vitest for an exact match, a difference with an explanation, an unexplained difference, a parent without "Sesuaikan saldo", and the matched point after adjusting.
- [x] 2.2 A browser check at 1440 light, 1440 dark and 390 dark: axe serious/critical 0, overflow 0, console errors 0, targets ≥ 24 px, focus trapped and returned. Evidence: screenshots read and recorded in verification.md.

Expected Result: the owner can check any liquid account from Rekening & Kantong.

## 3. Verification and delivery

- [x] 3.1 Backend pytest full. Frontend `npm ci`, type-check, lint, tests and build. Unchanged writes against `main`. `openspec validate --all --strict` and `git diff --check`. Evidence: verification.md.
- [x] 3.2 After the owner accepts:
  - archive the change;
  - push;
  - merge only after the V47 change (proposal, Impact);
  - report the PR link.
