# Tasks

## 1. Essential cost and coverage (D1, D2)

- [x] 1.1 Add observed periods (closed confirmed cycles, then complete calendar months after tracking began) and the essential-cost service (need-only, reimbursement share, exclusions, median, debt minimums, budget fallback, unknown). Evidence: `test_essential_cost.py` — unexpected month, need one-off, limited history, budget fallback, unknown is null, reimbursement apportionment, debt counted once, movements/investments/excluded categories, salary cycles versus calendar months, median rounding.
- [x] 1.2 Rewrite Ketahanan Dana on top of it: no sentinels, `balance_basis`, liquid fallback and owner multiplier in both endpoints, compatible fields never invented. Evidence: `test_coverage_never_invents_a_value`, `test_coverage_reports_its_balance_basis`, updated mock tests in `test_personal_finance_os.py` and `test_kakeibo_and_insights.py` (null instead of 999).
- [x] 1.3 Show coverage and its basis on Dompet (desktop and mobile): unknown state, basis sentence, limited-history label, all-liquid notice, period drill-down; drop the obsolete primary-category checkbox. Evidence: `EmergencyCoverage.test.tsx` with axe, updated `accounts/page.test.tsx` and `insights/page.test.tsx`.

Expected Result: the essential cost is stable against one-off months, shows its basis, and coverage is null rather than invented.

## 2. Automatic plan linking (D3)

- [x] 2.1 Migration V38: `link_method`, `obligation_applied`, rejections and audit events; existing links marked `owner`. Evidence: Flyway V1→V38 on a fresh PostgreSQL 16; `test_v38_marks_existing_links_as_owner_links_and_can_be_retried`.
- [x] 2.2 Auto-link exactly one certain payment, reuse the link path for debt progress, write audit events, refuse candidates claimed by two plans; advance past resolved occurrences. Evidence: `test_plan_auto_link.py` — one candidate linked once, concurrent passes, two candidates, wrong account/amount/window, competing plans, income versus transfer plans, resolving after an undo.
- [x] 2.3 Owner undo endpoint: reverse debt progress once, remember the rejection, return the occurrence to due. Evidence: `test_undo_reverses_the_debt_once_and_is_never_relinked`, `test_undo_does_not_reverse_debt_the_link_did_not_apply`.
- [x] 2.4 Triggers after recording commits and in the periodic loop, with a kill switch. Evidence: `test_recording_requests_a_link_pass_and_the_request_links`, `test_disabled_auto_linking_ignores_requests`, `test_plan_sweep_shares_the_periodic_loop_and_pages_independently`, `test_plan_link_requests_wake_the_request_loop`.
- [x] 2.5 Due-plan panel: "Ditautkan otomatis" list with "Batalkan tautan", manual candidates unchanged. Evidence: `PendingScheduledBanner.test.tsx` (label, undo call, error, axe).

Expected Result: an obviously captured payment meets its plan without owner work, and every automatic link is visible and reversible.

## 3. Observed insights (D4)

- [x] 3.1 Endpoints for the fair comparison, cash-flow history and recurring charges. Evidence: `test_observed_insights.py` — same elapsed days, no previous data, shortened previous cycle, history with period in progress, monthly and weekly detection, irregular merchant not detected, price change, ended charge, existing plan marked, "Jadikan rencana" creates a non-posting plan.
- [x] 3.2 Analisis comparison tab with the three insights and drill-down; plan form prefill from a detected charge; ledger `from`/`to`/`q` URL filters. Evidence: `insights/page.test.tsx`, `RecurringRulesModal.test.tsx`, `ledgerFilters.test.ts`, axe.

Expected Result: Analisis shows observed comparisons, per-period cash flow and recurring charges, each traceable to ledger rows.

## 4. Verification and delivery

- [x] 4.1 Backend suite on a fresh PostgreSQL 16 migrated V1→V38 and a CI-style run without a database. Evidence: pass counts in verification.md.
- [x] 4.2 Frontend `npm ci`, test, lint, type-check, build. Evidence: counts in verification.md.
- [x] 4.3 Browser checks with headless Brave over CDP at 390 and 1280 px, light and dark, keyboard and console, on synthetic data. Evidence: verification.md.
- [x] 4.4 `openspec validate --all --strict` and `git diff --check`; final diff review. Evidence: command output in verification.md.
- [x] 4.5 Hand-off: commit locally on `feat/essential-cost-and-insights`. Pushing and the compare link are left to the owner (not authorised in this delegated run); archive only after `audit-financial-calculations-and-automation`. Evidence: `git log` on the branch.

Expected Result: verified local commits ready for owner acceptance.

## Follow-ups (not in this change)

- Irregular and annual essential costs (sinking-fund plans).
- Android: no change required; the companion reads only `/dashboard/cash-flow-summary`.
