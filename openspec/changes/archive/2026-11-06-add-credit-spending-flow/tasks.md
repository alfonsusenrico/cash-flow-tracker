Preconditions:
- `add-language-system` (V53) and `localize-server-text` (error contract, server catalogues, `label_source`) are merged.
- This branch is rebased on that `main`.
- The owner has reviewed flag F1 (proposal, design D9).

Database tests run on a disposable PostgreSQL 16 migrated with Flyway from V1, as CI does, using synthetic owners and amounts only. A skipped database test counts as not run, and it is reported as such.

## 1. Migration V54

- [x] 1.1 Write `db/migrations/V54__credit_purchase_terms.sql` (design D1, D2, D8, D15). It:
  - adds `accounts.statement_day` with its CHECK;
  - creates `credit_purchase_terms` with its CHECKs, the cascade and the `(user_id)` index;
  - drops and re-adds `categories_system_key_check` with `credit_fee`;
  - backfills `credit_fee` on expense categories named exactly "Biaya kredit", with V52's duplicate guard.
  Evidence: `flyway migrate` from V1 on a fresh database reports schema version 54. Running the file a second time on a V54 schema succeeds and changes nothing.
- [x] 1.2 Migration test on synthetic data (`tests/test_credit_purchase_terms_migration.py`). It migrates a throwaway schema to V53 and seeds:
  - two owners with credit and bank accounts;
  - an expense "Biaya kredit" with a fee row, an income "Biaya kredit", a lower-case "biaya kredit" and a renamed fee category;
  then applies V54.
  Evidence: the test asserts that:
  - only the expense "Biaya kredit" gets `credit_fee`;
  - `statement_day` is NULL everywhere;
  - the terms table is empty;
  - the CHECKs reject `statement_day` 0 and 32, `installment_count` 0 and 37, and a due date without its setter;
  - deleting a transaction deletes its terms;
  - every Beranda, ledger, analytics and report figure of both owners equals its value at V53 (figure parity).
- [x] 1.3 Rollback check. Evidence: the previous release's backend suite subset `test_credit_accounts.py` and `test_category_system_keys.py`, run against a V54 schema, passes. This proves that an older release works on the new schema.
- [x] 1.4 Version uniqueness guard. Evidence: a test asserts that every `db/migrations/V*.sql` version is unique and that V54 directly follows the highest earlier version. A clash found on rebase is resolved by renaming this file to the next free number before merge (D15), and the test passes after the rename.

Expected Result: V54 applies cleanly from V1 and over V53 synthetic data, changes no figure, and is safe for the previous release.

## 2. Credit fee identity

- [x] 2.1 Add `CREDIT_FEE` to `services/system_categories.py` as a reportable key, with seed names per language ("Biaya kredit", "Credit fee"). Change `ensure_credit_fee_category` to `ensure_system_category` (D8). Evidence:
  - `test_system_category_guard.py` passes with the new key;
  - a test books a fee for an `id` and an `en` account and asserts the names and the key;
  - a renamed fee category is still reused, and no second category is created.
- [x] 2.2 Fee parity. Evidence: `test_credit_accounts.py::test_a_payment_with_interest_books_the_fee_as_its_own_expense` and the fee split and delete tests pass unchanged. A new test asserts that Pengeluaran, Kakeibo `unexpected` and the report outflow are identical before and after renaming the fee category.

Expected Result: fees are found by key, created in the account's language, and still count as spending.

## 3. Purchase terms and due dates

- [x] 3.1 `services/credit_terms.py`, which covers:
  - the statement close and first due date (D2), with the default bill day 1 and clamping;
  - instalment dates and the whole-rupiah split (D7);
  - owner first due dates.
  Evidence: unit tests `tests/test_credit_terms.py` cover every Credit Due Dates scenario:
  - 5 Nov and 1 Dec on (1, 11);
  - 20 and 25 Nov on (25, 5);
  - Rp 1.000.000 in 3;
  - an owner date of 1 Dec;
  - 31 January with payment day 31 in February;
  - no payment day.
- [x] 3.2 Accept `statement_day` on account create and edit (credit only). Evidence:
  - an API test for the "Bill day" scenario;
  - `credit_fields_not_allowed` on a bank account;
  - another owner's account gives 404.
- [x] 3.3 Terms endpoints: `PUT` and `DELETE /api/transactions/{id}/credit-terms`, and `credit_terms` on `POST /api/transactions`. They lock the purchase row and refuse with the new codes. Evidence: API contract tests for every Credit Purchase Terms scenario, including:
  - 422 codes with `{code, params, message}`;
  - 404 for another owner's transaction;
  - the response schema;
  - every code present in both server catalogues and the web `errors` namespace (the `localize-server-text` catalogue test).
- [x] 3.4 Terms follow edits. Moving the purchase off credit or changing its type deletes the terms. A date change clears a `system` due date, or an owner due date before the new date, and returns `credit_terms_reset` (D1). Evidence: API tests for "Purchase moved off the credit account" and "Purchase date moved past the owner's due date". Transaction delete, undo and merge leave no orphan terms (a query asserts zero rows without a transaction).
- [x] 3.5 Keep due dates on a day change: the account `PATCH` writes `system` first due dates for purchases due on or before today (D2). Evidence: an API test for "Payment day moves from 11 to 5", with a frozen clock, asserts that the paid bill stays on time and the open bill moves.
- [x] 3.6 Terms never move figures. Evidence: a test records a 6-instalment purchase, changes it to 3 and resets it. After each step, Pengeluaran, Kakeibo, Aman dipakai, the report outflow and the ledger totals equal the paid-in-full values.

Expected Result: every credit purchase has terms and due dates, set and edited only through validated, owner-scoped calls, with no figure changed.

## 4. Bills, settlement and the overview

- [x] 4.1 `services/credit_bills.py`, which builds:
  - the items (instalments, fees, cash advances, opening debt) and credits (payments, refunds) of one account;
  - oldest-due-first settlement with `settled_on`, `days_to_settle` and `settled_on_time` (D4, D5, D6);
  - bills with their status and statement date.
  It raises an error if the invariant `Σ items − Σ credits = −balance` fails. Evidence: `tests/test_credit_bills.py` covers every Credit Bills, Payments Settle the Oldest Due First and Settlement Facts per Item scenario, with a frozen clock:
  - the December bill;
  - opening debt;
  - overdue;
  - two-part payment;
  - early payment in full;
  - payment with a fee;
  - overpayment;
  - refund;
  - cash advance;
  - 35 days.
- [x] 4.2 Invariant property test. Evidence: a seeded random sequence of 500 creates, edits, deletes, splits, refunds and payments on two credit accounts asserts `Σ left_to_pay − credit_balance = credit_owed` after every step, for both accounts.
- [x] 4.3 `routers/credit.py`: `GET /api/credit/accounts/{id}` (with `from`/`to`) and `GET /api/credit/bills` (with `from`, `to` and `status`). Evidence: API contract tests for every Credit Account Overview scenario:
  - the totals;
  - bills across accounts;
  - 422 `credit_account_required`;
  - 404 for another owner;
  - `due_dates_unavailable`;
  - the English error message;
  - the response schemas.
- [x] 4.4 Read-time cost (risk R3). Evidence: a test with 2,000 synthetic rows on one credit account measures `GET /api/credit/accounts/{id}` locally under 300 ms. The measured time is recorded in `verification.md`.

Expected Result: bills, settlement facts and the overview are computed on read, always add up to what is owed, and are owner-scoped.

## 5. Ledger rows and accounts

- [x] 5.1 Ledger rows gain the `credit` block, computed once per credit account present on the page. Evidence: API contract tests for every Credit Facts on Ledger Rows scenario (purchase, payment as one movement row, per-provider account filter, ordinary row null). A query-count test shows one schedule build per credit account per request.
- [x] 5.2 Credit rows of `GET /api/accounts` gain `statement_day`, `limit_left` and `next_due`. Evidence: an API contract test asserts the fields for credit rows (including a negative `limit_left` over the limit) and their absence or null for other types. `test_credit_accounts.py` passes unchanged.

Expected Result: every surface the Markas+ phases render has its credit facts in existing responses, with only additive fields.

## 6. Cycle summary figures and the double count

- [x] 6.1 Add `spending.credit` (`spent_on_credit`, `paid_to_credit` and lines with `label_source: user`) (D11). Evidence: tests for the "Purchase and payment in one cycle" and "No credit accounts" scenarios. `spending`, `keluar`, `masuk`, `fresh_saving` and `remaining` are byte-identical to a baseline run.
- [x] 6.2 F1: mark credit-bill plan lines with `plan_amount`, a counted `amount` capped by the opening debt left to pay in due-date order, and `counted_elsewhere`. Sum Terjadwal and `plans_total` over `amount` (D9). Evidence: a **double-count test**: a Rp 470.000 purchase plus a Rp 470.000 transfer plan into the credit account lowers Aman dipakai by Rp 470.000, not Rp 940.000. Paying and confirming the plan then leaves Aman dipakai unchanged. The opening-debt scenario counts Rp 200.000 and Rp 100.000.
- [x] 6.3 Figure parity for accounts without credit-bill plans or terms. Evidence: a parity test on the synthetic parity seed (owners with no credit, credit with purchases only, and credit with purchases and payments) asserts that every cycle-summary figure, Analisis figure, ledger total and monthly report figure equals the baseline from `main`.
- [x] 6.4 F2 characterisation. Evidence: a test moves Rp 200.000 into a role-`tabungan` pocket, then from it into a credit account. It asserts today's rule: "Ditabung & investasi" rises and then falls by Rp 200.000, and `paid_to_credit` grows by Rp 200.000. The "Payment from a savings account" scenario holds.

Expected Result: Beranda's data carries the two credit figures, Terjadwal no longer counts credit purchases twice, and every other figure is unchanged.

## 7. Integration checks

- [x] 7.1 Full backend suite as CI runs it. Evidence: `flyway migrate` from V1, then `python -m pytest tests/ -ra` in `backend/` passes. Counts, skips and the revision are recorded in `verification.md`.
- [x] 7.2 Frontend unaffected. Evidence: `npm run type-check`, `npm run lint`, `npm test` and `npm run build` in `frontend/` pass with no frontend diff. The generated API types, if any, gain only optional fields.
  - Done with one frontend diff: the new error codes in the web `errors` namespace of `src/messages/{en,id}.json`, which 3.3 requires, and the bill day in `credit_fields_not_allowed`. No type, client or component changed (verification.md).
- [x] 7.3 Hand-off ready. Evidence: design D12's blocks H5, H8, H9 and H11 are checked against the final API field names. `PROJECT_STATE.md` lists, for each phase, the block it must add to its own change, and that phase 5's `apply-markas-transaksi` gains H5 before it starts.
  - The hand-off list is in verification.md. `PROJECT_STATE.md` is untracked and lives in the owner's checkout, which this worktree's work may not touch, so the list is handed to the coordinator to copy there.
- [x] 7.4 Specs and diff. Evidence:
  - `openspec validate add-credit-spending-flow --strict` and `openspec validate --all --strict` pass;
  - `git diff --check` is clean;
  - the reviewed diff has no secrets, dead code or unrequested changes (`change-review`);
  - `verification.md` is complete.

Expected Result: the change is verified locally end to end and is ready for the owner's acceptance.

## Workflow follow-up

- After the owner accepts: run `openspec archive add-credit-spending-flow`, then `openspec validate --all --strict`, and record both exit statuses.
- Commit on `feat/credit-spending-flow` with subject-only Conventional Commits, for example `feat(credit): add purchase terms, bills and settlement facts`.
- Push the branch. When the owner says "open", create the pull request with `gh pr create` into `main`, with specs and code together, and report its URL. Never approve or merge it.
- After merge: delete the branch locally and on the remote, and remove the `cash-flow-tracker-credit-flow` worktree.
- Production confirmation, and re-asking for SPayLater and Kredivo notification samples (D13), are tracked in `PROJECT_STATE.md`, not here.
