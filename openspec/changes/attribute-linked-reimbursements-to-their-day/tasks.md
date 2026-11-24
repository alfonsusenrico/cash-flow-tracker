## 1. Backend

- [x] 1.1 Linked parts per expense day, category and pillar from the offset loader; `spending_basis` subtracts them on that day and spreads only the unlinked and rest offsets. Evidence: tests for "A repaid meal stays on its day" and "An unlinked reimbursement still spreads"; days, categories and pillars add up to unchanged totals.
- [x] 1.2 Every daily series on the same basis (`daily_cadence`, Kalender days, heatmap, trendline) follows. Evidence: the existing reimbursement, analytics and calendar tests pass with unchanged totals; a test that Kalender's day total equals the analytics day.

Expected Result: a day can be checked against its own rows, and no figure outside the days changes.

## 2. Verify and deliver

- [x] 2.1 Full backend and frontend suites; `openspec validate --all --strict`; a smoke check of Kalender and Ringkasan on a production build; `verification.md`.
- [ ] 2.2 After the owner accepts: archive, push, open the PR and report its URL.
