## 1. Transaction titles

- [x] 1.1 `rowTitle` and `rowMeta` follow the title rule; the edit dialog subtitle too. Evidence: `rows` tests for note and merchant, a note naming the merchant, merchant only, category only, movements unchanged; the suites of every row user pass.

Expected Result: rows read by their note.

## 2. Figures open their detail

- [x] 2.1 `FigureBlock` link mode with focus and hover states. Evidence: component test (role link, name, href) and axe.
- [x] 2.2 `BerandaHero` links for cycle and month bases. Evidence: `BerandaHero` tests for each href in both bases and the keyboard.

Expected Result: every supporting figure opens its filtered detail.

## 3. Credit bills figure and dialog

- [x] 3.1 The "Tagihan kredit" figure: sum, nearest due caption, nothing owed, no credit account, hidden balances, failure. Evidence: tests.
- [x] 3.2 `CreditBillsDialog`: order, header facts, rows from the oldest unpaid item, payments, nothing owed, one section failing, focus. Evidence: tests and axe.
- [ ] 3.3 Browser check on a production build with synthetic data: Beranda at 1440, 1024 and 390 in light and dark with the dialog open; figure-to-Transaksi parity for Masuk, Pengeluaran and Ditabung & investasi; no horizontal overflow; captions on one line. Evidence: results in `verification.md`, captures shown to the owner and the owner's answer recorded.

Expected Result: credit bills are visible on Beranda and explained per account.

## 4. Verify and deliver

- [x] 4.1 Frontend type check, lint, tests and build as CI runs them; `openspec validate --all --strict`. Evidence: `verification.md`.
- [ ] 4.2 After the owner accepts, archive the change, push `feat/beranda-figure-details`, open the PR and report its URL.
