## 1. Backend

- [ ] 1.1 `by_pillar` and `by_category` on every `daily_cadence` day from the spending basis. Evidence: a test that both add up to `spending` on every day of a seeded cycle, including a reimbursed day; existing analytics tests pass.

Expected Result: the API explains each day's Pengeluaran.

## 2. Web

- [ ] 2.1 Bars as buttons opening Kalender at their day (click, Enter/Space, touch link). Evidence: chart tests for each input and the URL.
- [ ] 2.2 "Per pilar | Per kategori" table with fixed date column and zero cells; messages in both languages. Evidence: table tests for both modes, fewer than six categories, and the remembered choice.
- [ ] 2.3 Comparison row label column. Evidence: browser check at 1280 px.
- [ ] 2.4 Browser check on a production build with synthetic data at 1440, 1024 and 390 px, light and dark. Evidence: captures, axe, no page-level horizontal scroll.

Expected Result: a day's spending can be explained and opened from the chart.

## 3. Verify and deliver

- [ ] 3.1 Full backend and frontend suites; `openspec validate --all --strict`; `verification.md`.
- [ ] 3.2 After the owner accepts: archive, push, open the PR and report its URL.
