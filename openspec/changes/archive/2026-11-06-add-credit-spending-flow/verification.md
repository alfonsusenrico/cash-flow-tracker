# Verification: add-credit-spending-flow

Date: 2026-11-06. Branch `feat/credit-spending-flow`. Implementation commit `5b07ef8` on top of `origin/feat/language-system` `231f6a6` (V53 and the `{code, params, message}` error shape). All data is synthetic, on disposable PostgreSQL 16 (`postgres:16`) migrated with `flyway/flyway:10`. No production system was touched.

## Owner acceptance

- Approval and acceptance: the owner's standing instruction "complete all until finished" (2026-11-06) covers this change, its implementation and its archive.
- Flag F1: the owner approved it explicitly on 2026-11-06 with "ok". A transfer plan into a credit account counts in Terjadwal only up to the account's opening debt still left to pay; the rest is shown as already counted in Pengeluaran.
- O1–O5 and every other decision take their recommended defaults (proposal, design D16).

## Merges

- `origin/feat/language-system` was merged twice, at `76c63d1` and at `231f6a6` (the npm 10 lockfile fix). Both merges were fast-forwards, because this branch had no commit of its own yet, so no merge commit exists to name.
- `feat/localize-server-text` was not pushed by the time of this verification (`git ls-remote origin feat/localize-server-text` returned nothing), so it is not merged. The new refusals follow the `add-language-system` contract (`app/core/errors.coded_error`) with English and Indonesian messages in `services/credit_terms.MESSAGES`, and every new code is in the web `errors` namespace. When `localize-server-text` lands, these messages move into its catalogues and `ErrorCode`.

## Checks

| Check | Result |
| --- | --- |
| Flyway V1→V54 on a fresh database (`ledger_ci`) | Schema version 54; a second `migrate` reports "up to date". |
| V54 run twice on a V54 schema | No change (`test_credit_purchase_terms_migration`). |
| Full backend suite, `python -m pytest tests/ -ra` at `5b07ef8` on `ledger_ci` | 2056 passed, 0 failed, 0 skipped (1990 before this change). |
| New tests | `test_credit_terms.py` 13, `test_credit_bills.py` 17, `test_credit_spending_flow.py` 33, `test_credit_purchase_terms_migration.py` 3. |
| Rollback (1.3): the previous release's `test_credit_accounts.py` and `test_category_system_keys.py` (from `76c63d1`) against the V54 schema | 38 passed. |
| Figure parity before and after (6.3), `scripts/verify/credit_figure_parity.py` | 3 owners, 63 endpoint responses, 0 differences. Evidence: `research/evidence/add-credit-spending-flow/figure-parity.json`. A negative control without removing the added fields finds 145 differences, so the comparison is live. |
| Read-time cost (4.4): `GET /api/credit/accounts/{id}` with 2,000 rows | Median 45.1 ms of five reads (44.2–76.5 ms), under 300 ms. |
| Frontend `npm run type-check`, `npm run lint`, `npm test`, `npm run build` (npm 10 `npm ci`) | All exit 0; 94 test files, 900 tests passed. |
| `openspec validate add-credit-spending-flow --strict`, `openspec validate --all --strict` | Valid; 86 passed, 0 failed. |
| `git diff --check` | Clean. |
| Archive: `openspec archive add-credit-spending-flow -y`, then `openspec validate --all --strict` | Archived as `2026-11-06-add-credit-spending-flow` (three specs updated: 9 added, 3 modified requirements); validation 85 passed, 0 failed, exit 0. No spec has a "TBD" Purpose. |

### Figure parity

The baseline snapshot ran the backend of `76c63d1` on a database migrated to V53; the second ran this change on V54. The seed has an owner without credit, one with credit purchases and a refund only, and one with purchases, a payment with a fee, a payment from a savings pocket and a cash advance. Every figure endpoint was read: pulse, insights, the overview for cycle and month, the cycle summary, analytics, cash-flow summary, net worth, the monthly report and its periods, review summary, accounts, plans, categories and the ledger tabs. The fields this change adds (`spending.credit`, the row `credit` block, `statement_day`, `limit_left`, `next_due`, and the `credit_fee` key) are removed before comparing. Identifiers and the run's instants are normalised.

### F1, the double count

- `test_f1_credit_bill_plan_is_not_counted_twice`: a Rp 470.000 SPayLater purchase lowers Aman dipakai by Rp 470.000. Adding a Rp 470.000 transfer plan Bank → SPayLater due in three days leaves it unchanged: the line shows `plan_amount` 470.000, `amount` 0 and `counted_elsewhere` 470.000, so the total fall is Rp 470.000, not Rp 940.000. Paying Rp 470.000 and linking the occurrence leaves Aman dipakai unchanged again.
- `test_f1_worked_numbers_from_the_research_note`: Masuk Rp 10.000.000, cash spending Rp 7.000.000, SPayLater Rp 2.000.000 and a Rp 2.000.000 "Bayar SPayLater" plan. Keluar is Rp 9.000.000, Terjadwal Rp 0 and Aman dipakai Rp 1.000.000. Before F1 it was Rp −1.000.000.
- `test_f1_counts_plans_for_opening_debt_in_due_date_order`: Kredivo added owing Rp 300.000 with weekly Rp 200.000 plans counts Rp 200.000 and Rp 100.000, and the rest as counted elsewhere; Terjadwal is Rp 300.000.
- `test_plans_into_other_accounts_keep_their_line_unchanged`: a transfer plan into an e-wallet keeps its line byte for byte.

### Other named evidence

- Savings payment characterisation (F2): `test_f2_payment_from_a_savings_pocket`. Rp 200.000 into a role-`tabungan` pocket raises "Ditabung & investasi" by Rp 200.000; paying the credit account from it lowers it by Rp 200.000, and `paid_to_credit` grows by Rp 200.000.
- `credit_fee` parity: `test_renaming_the_fee_category_moves_no_figure_and_is_still_reused` (Pengeluaran, Kakeibo unexpected and the monthly report are identical after a rename, and the renamed category is reused); `test_an_english_account_books_its_first_fee_as_credit_fee`; the unchanged `test_credit_accounts.py` fee tests; the V54 migration test keys only the exact expense "Biaya kredit".
- Invariant: `test_bills_always_add_up_to_what_is_owed` runs 500 seeded random writes (purchases with terms, term changes and resets, amount and date edits, deletes, refunds, payments with and without fees, movement splits and deletes) on two credit accounts and checks after each one that Σ left to pay − overpayment equals −balance, and that no orphan terms remain. `build_schedule` also raises `CreditInvariantError` on every read if the rule fails.
- Partial, early and over-payment: `test_partial_early_and_over_payment` (API) and the `test_credit_bills.py` cases for two-part, early-in-full, overpayment, payment before purchase, refund and fee.
- API contracts: every Credit Purchase Terms, Credit Due Dates, Due Dates Kept After a Day Change, Credit Bills, Settlement, Ledger Rows and Overview scenario has a test in `test_credit_spending_flow.py`, `test_credit_bills.py` or `test_credit_terms.py`, including 404 for another owner, the `{code, params, message}` shape, English and Indonesian messages, response key sets, and one schedule build per credit account per ledger request.

## Hand-off to the Markas+ phases (D12)

Checked against the final API field names. Each phase adds its block to its own change before it starts; phase 5's `apply-markas-transaksi` gains H5.

- **H5, phase 5 Transaksi:** ledger rows `credit.role`, `account_name`, `installment_count`, `installments_settled`, `next_due_date`, `settled_on`; payment rows `credit.role = payment`; the provider view is `GET /api/transactions?account_id=`; the provider header reads `GET /api/credit/accounts/{id}` (`account.owed`, `next_due`, `totals.spent_on_credit`, `totals.paid_to_credit`); the dialog reads `GET /api/transactions/{id}` `credit.installments` and writes `PUT /api/transactions/{id}/credit-terms`.
- **H8, phase 8 Rekening:** `GET /api/accounts` credit rows `credit_owed`, `limit_left`, `over_limit`, `next_due`, `statement_day`, `payment_day`; the detail reads `bills[]` with `status`; the form writes `statement_day` and `payment_day`.
- **H9, phase 9 Target & Tagihan, Beranda and the bell:** `GET /api/credit/bills?to=&status=` with `account_name` and `left_to_pay`; credit-bill plan lines carry `plan_amount` and `counted_elsewhere`; Beranda's Rincian reads `spending.credit.spent_on_credit`, `paid_to_credit` and `lines`.
- **H11, phase 11 Catat:** `POST /api/transactions` `credit_terms {installment_count, first_due_date}`; "Bayar sisa tagihan" uses `next_due.left_to_pay`; the fee stays the movement's `fee`.

`PROJECT_STATE.md` is untracked and lives in the owner's checkout, which this work may not touch; this list is handed to the coordinator to record there.

## Deviations and remaining risks

- Design D16 records the implementation notes: a stored first due date anchors its series; opening debt follows the current payment day (risk R5); `spent_on_credit` leaves out the saving pillar and shares reimbursement offsets by amount; three extra error codes; the dialog's `credit` block; the fee category's classification is never repaired; the test owner fixture became an Indonesian account. None changes an existing figure, which the parity run confirms.
- The existing `add-credit-accounts` refusals, `credit_fields_not_allowed` among them, still answer `{code}` only, until `localize-server-text` converts every error site.
- Derived due dates can differ from a provider's (R1, O1), and a partial refund settles the oldest item rather than the refunded purchase (R2).
- The SPayLater and Kredivo notification capture (D13) stays pending on real samples.

## Merged with localize-server-text (2026-11-08)

`origin/feat/localize-server-text` (`af8f95e`, pull request #90, not merged yet) was merged at `92f2bc8`, then `8d15185` moved the credit text onto its contract. Every check below ran at `8d15185` with a clean tree apart from the new evidence file, on loopback with synthetic data in a disposable `postgres:16` migrated with `flyway/flyway:10`. No production system was touched.

### Conflicts

- `services/credit_accounts.py`: both import lines kept (`app.errors.api_error` and the `credit_fee` system category helpers).
- `routers/transactions.py`, `update_transaction`: the localized `notice("transaction_updated")` and this change's `credit_terms_reset` are both returned.
- Two conflicts that git did not flag: `app/core/errors.py` no longer exists, and `get_timeframe_window` now needs `language`, which the credit overview's default period now passes from the account. The full suite found the second one (three failures before the fix).

### Credit text on the shared contract

- Five codes were added to `ErrorCode` and to the server `en`/`id` catalogues: `credit_terms_not_credit_purchase`, `credit_terms_count_range`, `credit_terms_due_before_purchase`, `credit_account_required` and `credit_period_invalid`. The web `errors` namespace already held them. Their Indonesian text is the archived copy, unchanged. The two 404s use the existing `account_not_found` and `transaction_not_found`. `ErrorCode` now holds 301 codes, the same in all four catalogues.
- `credit_terms.MESSAGES` and `credit_error` are removed. Every credit refusal goes through `app/errors.api_error` and is rendered in the account's language. The terms helpers no longer take a language argument.
- The seven `add-credit-accounts` refusals (`credit_parent_not_allowed`, `credit_balance_sign`, `credit_fields_not_allowed`, `credit_type_change_has_transactions`, `credit_account_not_allowed`, `credit_fee_not_allowed`, `credit_movement_needs_cash`) were already converted by `localize-server-text` and now answer `{code, params, message}`. This replaces the earlier note that they answer `{code}` only. The server text of `credit_fields_not_allowed` now names the bill day, matching the web: "A limit, a bill day and a payment date are only for credit accounts." and "Limit, tanggal tagihan terbit, dan tanggal bayar hanya untuk rekening kredit.".
- The credit fee's note is now `notes.credit_fee` ("Credit fee", "Biaya kredit") in the account's language. It was the category seed name, which has the same text. Bill statuses stay codes that the web renders, and no other credit server text exists.

### Checks

| Check | Result |
| --- | --- |
| Flyway V1→V54 on a fresh database | Schema version 54; a second `migrate` reports "up to date". |
| Full backend suite, `python -m pytest tests/ -ra` (`SKIP_TEST_MIGRATIONS=1`, `REQUIRE_TEST_DATABASE=1`) | 2163 passed, 0 failed, 0 skipped (2097 from `localize-server-text` plus this change's 66). |
| Four-catalogue code parity, server catalogue parity, contract scan, every code renders, copy register (`test_server_text_language.py`) | 11 passed, the planted-problem controls among them. |
| Indonesian golden server text | 4 passed. |
| F1 (`test_f1_*` and `test_plans_into_other_accounts_keep_their_line_unchanged`) | 4 passed. |
| Credit language tests (`test_errors_are_in_the_accounts_language`, `test_every_credit_code_is_an_error_code_in_both_server_and_web_catalogues`, `test_an_english_account_books_its_first_fee_as_credit_fee`) | 3 passed. |
| Figure parity, `scripts/verify/credit_figure_parity.py`: `af8f95e` on V53 against `8d15185` on V54 | 3 owners without credit terms or credit-bill plans, 63 endpoint responses, 0 differences. The negative control without removing the added fields finds 145. Evidence: `research/evidence/add-credit-spending-flow/figure-parity-localize-merge.json`. |
| Frontend `npm run type-check`, `npm run lint`, `npm test`, `npm run build` | All exit 0; 94 test files, 904 tests passed. `package.json` and the lockfile are unchanged, so no `npm@10 ci --dry-run` was needed. |
| `openspec validate --all --strict` | 84 passed, 0 failed. |
| `git diff --check` | Clean. |

### Remaining risks

- This branch now contains `localize-server-text`. Merge pull request #90 first, or this branch's pull request carries both changes.
- Design D16's note that refusals use `credit_terms.credit_error` describes the state before this merge. This section supersedes it.
