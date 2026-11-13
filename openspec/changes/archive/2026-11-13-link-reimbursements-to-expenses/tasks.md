## 1. Data and server

- [x] 1.1 `V55__reimbursement_links.sql`. Evidence: Flyway V1→V55 on Postgres 16; the cascades are exercised by the delete tests in 1.2.
- [x] 1.2 Allocation and reallocation in `reimbursements`. Evidence: database tests for oldest-first allocation, two reimbursements on one expense, never below zero, edits and deletes on either side.
- [x] 1.3 `load_spending_offsets` applies linked amounts by expense period, category and pillar, then the capped category fallback. Evidence: tests for the scenarios "Repaid in the next cycle" and "Only the expense it repaid" read through `load_spending_offsets`, the one function the cycle summary, the ledger summary, analytics and the monthly report share; the existing reimbursement tests of those pages still pass.
- [x] 1.4 `PUT`/`DELETE /api/transactions/{id}/reimburses`, the rest as income, validation, and links in `GET /api/transactions/{id}`. Evidence: API tests for each scenario of "Reimbursements Linked to Expenses" and "The Rest of a Linked Reimbursement", including another owner's rows and the balance unchanged.
- [x] 1.5 Full backend suite. Evidence: `verification.md`.

Expected Result: links change spending exactly where the expense is, and nothing else.

## 2. Web

- [x] 2.1 `ExpensePicker` and "Mengganti transaksi" in the edit dialog and Catat, with the rest choice; "Diganti Rp X" in the transaction dialog; messages in both languages. Evidence: component and page tests, keyboard and axe.
- [x] 2.2 Browser check on a production build with synthetic data at 1440 and 390, light and dark: linking, the rest as income, the expense's dialog. Evidence: `verification.md`, captures shown to the owner and the answer recorded.

Expected Result: the owner can link a reimbursement in a few taps and see it on both sides.

## 3. Verify and deliver

- [x] 3.1 Frontend type check, lint, tests and build; `openspec validate --all --strict`. Evidence: `verification.md`.
- [x] 3.2 After the owner accepts, archive, push `feat/link-reimbursements-to-expenses`, open the PR and report its URL.
- [ ] 3.3 After the owner merges and it is deployed: show the owner the exact relinks of their records (CG Fun, the Rp 100.000 from their mother, Mie Tore, main BL), apply them through the API with the owner's key once confirmed, verify read-only, delete the key file.
