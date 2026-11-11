## 1. Credit colour

- [x] 1.1 Add `--credit-fg` (`#8A5A06` light, `#FBBF24` dark) and `.c-credit`. Evidence: `tokens.test.ts` passes, and a test measures at least 4.5:1 on canvas, surface and surface-2 and 14 OKLab units from `--out-fg` in both themes.
- [x] 1.2 `RowAmount` uses the credit colour for a purchase with no `settled_on`. Evidence: `rows.test.tsx` covers unpaid, partly paid, fully paid, no payment day, a bank expense, and hidden balances.
- [x] 1.3 Owner check: captures of Transactions and Home at 1440 and 390 in light and dark, shown to the owner. Evidence: the owner's answer recorded in `verification.md`: accepted 2026-11-11.

Expected Result: unpaid credit purchases are amber, and the owner has accepted the shade.

## 2. What a payment paid

- [x] 2.1 `credit_bills`: `Credit.movement_id` and a function computing a credit's share per item from range overlap. Evidence: unit tests for the four-item bill, two payments sharing a purchase, overpayment, a refund between payments, and the invariant raising when shares do not add up. Done: 12 new tests in `test_credit_bills.py`.
- [x] 2.2 `GET /api/transactions/{id}` adds `settled_items` and `kept_for_later` for payment rows. Evidence: database tests for both legs, a foreign owner getting 404, no payment day, a deleted purchase, and the list endpoint carrying no `settled_items`. Done: 7 new tests in `test_credit_spending_flow.py`.
- [x] 2.3 Dialog list in `TransactionDialog`, with `en` and `id` messages, each row opening its purchase. Evidence: component tests including partial share, kept for later and keyboard activation; the copy-register and language-key tests pass. Done: 10 tests in `TransactionDialog.test.tsx`; vitest 1090 passed.

Expected Result: opening a payment shows what it paid, and the shares add up to the payment.

## 3. Verification

- [x] 3.1 Figure parity before and after, English and Indonesian, on a seeded database with credit data. Evidence: every Spending, safe-to-spend, balance and bill figure identical. Done: 0 figures differ across 69,522 API values and 505 page amounts.
- [x] 3.2 Update `transaksi-checklist.js` and `beranda-checklist.js` to expect `--credit-fg` on unpaid credit rows. Evidence: checklists pass in both languages. Done: 0 failed.
- [x] 3.3 Captures at 1440/1024/390/320, light and dark, id and en, with axe, console and overflow checks, and keyboard flow through the payment list. Evidence: counts in `verification.md`. Done: 272 captures, keyboard 96 of 96.
- [x] 3.4 Frontend type-check, lint, tests and build; backend pytest on `postgres:16` migrated V1 → V54; `openspec validate --all --strict`; `git diff --check`. Evidence: counts in `verification.md`. Done: all pass; backend 2254 passed.

Expected Result: every check passes and no figure changed.

## 4. Delivery

- [x] 4.1 Write `verification.md`, and show the owner the result. Evidence: the file and the owner's acceptance on 2026-11-11 ("use Opening balance, push and open the PR").
- [x] 4.2 After the owner accepts: archive the change, push, create the PR and report its URL. Evidence: the PR URL.
