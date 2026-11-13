## 1. A past word after a date

- [x] 1.1 Reproduce: "tgl 11 kemarin, beli pedigree spaylater kredit 47619" on 13 Nov raises `several_dates` on `origin/main` `0d37f69`. Evidence: parser run recorded in `verification.md`.
- [x] 1.2 `DAY_DATE` and `MONTH_DATE` absorb a following past word. Evidence: new cases in `test_capture_time.py` and `test_capture_parsing.py` for every scenario, failing before the change; all capture tests pass.

Expected Result: the owner's sentence reads as 11 November, Rp 47.619, expense.

## 2. Paying later in Catat

- [x] 2.1 `AccountSelectOptions` `creditOnly`. Evidence: component test.
- [x] 2.2 "Kapan bayar" in `ManualView`, with `en` and `id` messages. Evidence: `ManualView.test.tsx` covers choosing pay later, back to pay now with no `credit_terms`, and no choice without a credit account; keyboard selection; language-key and copy-register tests pass.
- [x] 2.3 Owner check: captures of Catat with each choice at 1440 and 390 in light and dark, shown to the owner. Evidence: the owner's answer recorded in `verification.md`.

Expected Result: credit is visible in Catat before choosing an account.

## 3. Verify and deliver

- [x] 3.1 Backend suite, frontend type check, lint, tests and build as CI runs them; `openspec validate --all --strict`. Evidence: results in `verification.md`.
- [x] 3.2 After the owner accepts, archive the change, push `fix/credit-capture-entry`, open the PR and report its URL.
