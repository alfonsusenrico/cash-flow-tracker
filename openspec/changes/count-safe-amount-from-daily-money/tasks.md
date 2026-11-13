## 1. Server

- [x] 1.1 `safe_amount` in `build_cycle_summary`: daily money from active harian liquid leaves, credit owed from active credit accounts, scheduled lines from daily money only; `amount`, `short_by`; `per_day` from `amount`; `remaining` removed. Evidence: database tests for every scenario of "Safe-to-Spend Today" and "Scheduled Amount", lines adding up, another owner's data, and the owner's 13 November figures as synthetic data (Rp 4.420.334 − 3.046.014 − 0 = 1.374.320).
- [x] 1.2 Full backend suite. Evidence: result in `verification.md`.

Expected Result: the API returns the cash-based safe amount with its parts.

## 2. Beranda

- [x] 2.1 Hero label, line and shortfall; split bar of daily money; Rincian with the three opening lines; the credit dialog's meta. Evidence: `page.test.tsx` for available, unknown and late salary, short, the bar's name, each Rincian line's parts, hidden balances, English, axe.
- [ ] 2.2 Browser check on a production build with synthetic data at 1440, 1024 and 390 in light and dark, Rincian open. Evidence: results in `verification.md`, captures shown to the owner and the answer recorded.

Expected Result: Beranda answers "how much can I still use safely".

## 3. Verify and deliver

- [x] 3.1 Frontend type check, lint, tests and build as CI runs them; `openspec validate --all --strict`. Evidence: `verification.md`.
- [ ] 3.2 After the owner accepts, archive the change, push `feat/cash-based-safe-amount`, open the PR and report its URL.
