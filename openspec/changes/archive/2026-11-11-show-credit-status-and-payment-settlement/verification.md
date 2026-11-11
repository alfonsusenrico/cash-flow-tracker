# Verification: show-credit-status-and-payment-settlement

Date: 2026-11-11 · Branch `feat/credit-status-and-payment-settlement` · Base `origin/main` `710d758` · Code under test
`1082946` unless a check names another revision. The commit that adds this file changes `openspec/` only.

## Owner's colour check (task 1.3)

Accepted by the owner on 2026-11-11 on the real pages: "amber is ok, continue the build". The owner saw that in light
mode the amber reads as a brownish ochre and that in dark it equals the Overdue label, and accepted both. The colour
values were not changed.

## What changed

- `credit_bills`: `Credit.movement_id`, `credit_shares` (a credit's share of each item as the overlap of the cumulative
  ranges, walking the lists `settle` already sorted, with no second ordering) and `payment_settlement`. The invariant
  raises when the shares and the part kept for later do not equal the credit.
- `GET /api/transactions/{id}`, for a payment on either leg: `settled_items`, `kept_for_later` and
  `due_dates_unavailable`. Schedules load for payment rows only in `with_installments` mode, so the list endpoint gains
  no query and no field. Titles come from one query for the listed items.
- The dialog lists the items: a whole-row button per item with a visible focus state, plain rows for opening debt and
  the part kept for later, amounts through `MaskedMoney`, and new `en` and `id` messages.
- `transaksi-checklist.js` and `beranda-checklist.js` expect `--credit-fg` on a row that says "pay later", and on a
  dialog with a "Due" fact.

## Deviations and notes for the owner

1. The design does not say where the list goes. It sits directly under the amount, above the facts: below them it was
   out of view on a phone, behind nine facts.
2. `due_dates_unavailable` is also sent as `false` when dates exist, as `purchase_facts` does. The design names only
   the `true` case.
3. The opening item is labelled "Opening balance" / "Saldo awal", matching the account's bill list (owner decision, 2026-11-11; the earlier captures show "Opening debt").
4. Opening a row moves focus to the new row's body, because the button that was pressed is gone. The existing "View fee
   transaction" link uses the same handler and gains this too.
5. The self-review found that a pending focus move survived a row answering 404. Fixed in `1082946`, with a regression
   test that fails without the fix.

## Checks

| Check | Revision | Result |
| --- | --- | --- |
| `npm run type-check` | `1082946` | passed |
| `npm run lint` | `1082946` | passed, no warnings |
| `npm test` (vitest) | `1082946` | 98 files, 1090 tests passed |
| `npm run build` (webpack) | `1082946` | passed |
| Backend pytest, fresh `postgres:16` migrated V1 → V54 with `flyway/flyway:10`, `uv venv -p 3.12`, `TEST_DATABASE_URL`, `SKIP_TEST_MIGRATIONS=1`, `REQUIRE_TEST_DATABASE=1` | `4e60299` (`git diff 4e60299 1082946 -- backend db` is empty) | 2254 passed |
| `openspec validate --all --strict` | final | 85 passed, 0 failed |
| `git diff --check origin/main` | final | clean |
| `frontend/package.json`, `package-lock.json` | `git diff 710d758 1082946` | unchanged, so no lockfile regeneration |

New tests: 12 unit tests in `test_credit_bills.py`, 7 database tests in `test_credit_spending_flow.py` and 10 component
tests in `TransactionDialog.test.tsx`, which cover task 2.1, 2.2 and 2.3's named evidence.

## Figure parity (task 3.1)

One database, migrated to V51, loaded with `seed_parity.py`, migrated to V54, then `seed_transaksi.py` and a credit seed
through the API (Kredivo Contoh with the four-item bill; Atome Contoh with opening debt, a fee, a partial share and an
overpayment). Cloned with `CREATE DATABASE … TEMPLATE` into four databases, two set to English. Before: `710d758`'s
backend and production build from a temporary worktree. After: this branch.

- API, 25 endpoints (cycle summary, overview, net worth, analytics, cash-flow summary and history, period comparison,
  recurring charges, flow, calendar, pulse, insights, accounts, bills, goals, obligations, each credit account, and
  transaction lists with their summaries): 69,522 values compared before against after, in both languages. 30 differ,
  and all 30 are the request's own clock (`as_of`, `end`). No figure differs. `/reports/latest` answers 404 in all four,
  so reports are covered by the page below.
- Details of 26 credit rows, in both languages: 3,052 values, 0 differences once the three new payment keys are set aside
  (28 payment details).
- Indonesian against English: 3,307 numbers, 0 differences.
- Pages (Home, five Transactions views, Accounts, Analysis, Goals, Reports): 505 amounts, identical before and after in
  both languages. The full page text is also identical, and the amounts match across languages.
- `parity-languages.mjs` (Home and its breakdown): SAME in all three scenes.

## Browser checks (tasks 3.2, 3.3)

272 captures of the final build: 136 per language at 1440/1024/390/320, light and dark (`dlg-rekening-kredit` is limited
by its scenario to 1440 and 390). Scenarios: Home, Transactions views, the credit purchase and payment dialogs, Accounts
detail, the calendar day, hidden balances, and five payment scenarios (four items, partial with a fee, partial with
opening debt and kept for later, hidden balances, a row opened). The five scenarios lived in a scratch copy of
`scripts/verify/markas/`; they are not committed.

| | Indonesian | English |
| --- | --- | --- |
| Transaksi checklist | 1942 checks, 0 failed | 1929 checks, 0 failed |
| Beranda checklist | 2700 checks, 0 failed | 2700 checks, 0 failed |
| Rekening and Analisis checklists | 298 checks, 0 failed | 298 checks, 0 failed |
| Horizontal overflow | 0 | 0 |
| Console errors | 2, see below | 0 |
| Axe, serious or critical, in the payment dialogs (40 captures) | 0 | 0 |
| Axe, serious or critical, elsewhere | 82 | 74 |

- The credit checks are not empty: Transaksi ran 72 passing credit checks per language, and Home shows the unpaid
  "Sepatu lari" row in `rgb(138, 90, 6)`.
- Console: two 502s from the scratch proxy on `/api/review/summary` and `/api/recurring/pending` while captures, keyboard
  flows and parity ran together. The two scenarios, re-run alone, logged none.
- Axe elsewhere: `label-content-name-mismatch` (152) and `target-size` (4), all outside the new list: `.nav-item`, the
  tab bar, `.user-btn`, calendar cells, the account's more-actions button, `.ws-linkbtn`. The same rules on the same
  elements appear on the `710d758` build (8 sample captures). Axe here is 4.14.0; the frontend's own is 4.11.4. Left as is.
- Keyboard flow through the payment list, at 1440 light and dark, 390 light and 320 dark, in both languages: 96 of 96
  passed. Enter opens the payment; Tab walks the four rows in view with a 2 px focus ring; Shift+Tab goes back; Enter and
  Space open the purchase in the same dialog; focus lands on the body; Esc closes with focus on the ledger row; plain
  rows are not tab stops; the credit account's own leg lists the same four rows.
- Images were reviewed by eye, including 320 dark, the phone sheet, hidden balances and an opened purchase.

## Not verified

- A real phone, a screen reader and 400 % zoom (320 px captures cover reflow).
- nginx: a loopback proxy stood in for it. No production system was touched.
- No AI or paid call: AI was disabled, no key was set, and outbound HTTP pointed at a closed port.

## For PROJECT_STATE.md

`PROJECT_STATE.md` is untracked in the owner's checkout, which this run does not touch; the coordinator carries these over:

- `show-credit-status-and-payment-settlement` is built through task 4.1 on `feat/credit-status-and-payment-settlement`,
  local only, not pushed, no PR, not archived. Next: the owner reviews; then task 4.2.
- Unpaid credit purchases are amber (`--credit-fg`), accepted by the owner 2026-11-11. A payment's dialog lists what it
  paid, computed from `credit_bills.credit_shares`, on the detail endpoint only.
- Owner decision, 2026-11-11: the payment list uses "Opening balance" / "Saldo awal", the same name as the account's bill list.
- Known: 156 axe findings outside this change on axe 4.14.0, also present on `710d758`.
