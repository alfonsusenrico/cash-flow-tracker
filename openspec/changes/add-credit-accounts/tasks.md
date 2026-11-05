## 1. Schema and accounts API

- [x] 1.1 V50: add `credit_limit` and `payment_day` with CHECKs, additive and safe to run twice. Evidence: Flyway V1 → V50 on a disposable PostgreSQL, and a rerun of the file is a no-op.
- [x] 1.2 Accounts API: type `credit`, the sign rule, no parent or pockets, credit-only fields, type change only without transactions, and new row fields (`credit_owed`, `over_limit`). Evidence: API tests for create, edit and each 422 code, plus another owner's 404.
- [x] 1.3 Balance buckets (`credit_owed`, credit kept out of `investment_balance`) and net worth (liabilities and lines). Evidence: tests for the net-worth scenario "Credit owed is utang", "Same figure as Beranda" with a credit account, and overpaid credit counting 0.

Expected Result: credit accounts exist with honest balances and net worth.

## 2. Purchases, payments and fees

- [x] 2.1 Expenses and refunds on credit skip the funds check, with `over_limit` returned. Evidence: tests for the dinner scenario (Pengeluaran, pillar, "Aman dipakai", no liquid change) and the over-limit save.
- [x] 2.2 Movements into and out of credit, with an optional fee in "Biaya kredit". Evidence: tests for the Bayar Kredit payment (owed 0, cycle figures unchanged, not Ditabung), the payment with interest, a cash advance, and an insufficient bank side refused.
- [x] 2.3 Tinjau completion, merge and server pairing checks accept a credit side, and still require one liquid leg. Evidence: test for completing a Jago debit with SPayLater, and a credit-to-credit movement refused.
- [x] 2.4 Refusals: investment funding, obligation payment and "Cocokkan saldo" with a credit account. Evidence: a 422 test each.

Expected Result: spending counts once, at purchase time, and paying the bill is a movement.

## 3. Web

- [x] 3.1 Account dialog: the "Kredit" type with "Utang saat ini", "Limit" and "Tanggal bayar", with no pocket option for credit. Evidence: component tests for create and edit and the field rules, with axe.
- [x] 3.2 Rekening & Kantong credit card: "Utang Rp X", limit used, "dibayar tiap tgl N", "Melewati limit", "Lebih bayar", and no "Cocokkan saldo". Evidence: component tests.
- [x] 3.3 Pickers with a "Kredit" group (D7), and a fee field when the movement target is credit. Evidence: tests for QuickCapture, the movement modal and Tinjau completion.
- [x] 3.4 Beranda net-worth caption with credit in utang. Evidence: a Beranda test.
- [x] 3.5 Frontend checks. Evidence: `npm run type-check`, `npm run lint`, `npm test` and `npm run build`.
- [x] 3.6 Browser check on a local synthetic stack, at 1440 px and 390 px, light and dark, with axe: add SPayLater, record a purchase, pay the bill with a fee, and check Beranda and Rekening figures. Evidence: screenshots and figures summarised in `verification.md`.

Expected Result: the owner can run the SPayLater cycle end to end in the web app.

## 4. Delivery

- [x] 4.1 Run the full backend suite on migrated PostgreSQL, write `verification.md`, run `openspec validate --all --strict` and `git diff --check`, and review the diff.
- [ ] 4.2 After the owner accepts, archive the change, push the branch and report the compare link. After the merge, check V50 and the deploy read-only.
