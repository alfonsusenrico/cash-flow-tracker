## 1. Backend

- [x] 1.1 `type=saving` also lists top-up legs and Investasi rows; `type_counts.saving` follows; `saving_net` and `type=investment` unchanged. Evidence: API tests for a savings-funded top-up, an RDN top-up and the counts; the "Aggregate unchanged" test still passes.
- [x] 1.2 Summary `moved`. Evidence: a test that `moved` equals the sum of the day totals' `movements` over three pages, with and without an account filter.
- [x] 1.3 Running cycle `label_end_date` and "{start} – {end}" labels in both languages for the ledger, Analisis, Beranda and Laporan. Evidence: tests for the payday setting, regular receipts, a late salary and an early salary; report windows unchanged.

Expected Result: the API gives every type a meaningful figure and every cycle a date range.

## 2. Web

- [x] 2.1 Two-line transaction rows (`ListRow` stacked layout from `LedgerList`). Evidence: row tests for one and two lines and the order of meta, note and tags.
- [x] 2.2 A dot on every pillar cell. Evidence: `rows` tests for each kind.
- [x] 2.3 Types without "Investasi", the old link read as `saving`; hero for Masuk and Pindah saldo. Evidence: rail, filter and hero tests.
- [x] 2.4 Cycle labels from `label_end_date` in the stepper and Analisis; messages in both languages. Evidence: stepper and period tests.
- [x] 2.5 Browser check on a production build with synthetic data at 1440, 1280, 1024 and 390, light and dark: long names with notes and tags, every pillar kind, each type's hero, the period label. Evidence: captures, axe, no horizontal scroll.

Expected Result: names are readable, rows line up, and every figure on the page means something.

## 3. Verify and deliver

- [x] 3.1 Full backend and frontend suites; `openspec validate --all --strict`; `verification.md`.
- [x] 3.2 After the owner accepts: archive, push, open the PR and report its URL.
