# Tasks

## 1. Baseline and reporting contracts

- [x] 1.1 Establish a clean topic branch from the current merged target without disturbing the existing dirty checkout; compare all affected current main specs, including mobile, obligations, investment, pulse and report windows, and record every conflict and compatibility decision in this change before code changes. Verify with `git status` and OpenSpec validation.
- [x] 1.2 Inventory all displayed financial measures, consumers, their timezones, filters, transfer and reimbursement semantics, and existing API clients; document expected behavior and regression cases, verified against code and specs.
- [x] 1.3 Create a shared ledger reporting contract and tests covering fees, paired movements, reimbursement offsets, filters, dates and missing data; verify backend and ledger figures agree for identical synthetic datasets.
- [x] 1.4 Adopt shared reporting in web views with period labels and drill-down; verify comparison, date-boundary, accessibility and responsive tests.
- [x] 1.5 Integrate the server summary into the separate Android repository with explicit cached freshness and offline handling; verify identical synthetic web/mobile figures and no local/server double count on a test device. Implemented and JVM-verified in companion `2ea4b5e` (2.2.0); the device run is outstanding because the Realme was locked (see verification.md).

## 2. Salary cycles

- [x] 2.1 Add owner-confirmed salary-source and receipt state with non-destructive migration; verify candidate matching, bonuses, refunds, manual selection, corrections and repeat requests using synthetic data.
- [x] 2.2 Automatically confirm unambiguous explicit salary matches from the confirmed source, implement cycle boundaries and update all period consumers, preserving calendar navigation and unknown current cycle ends; verify late/missing salary, bonus/refund ambiguity, duplicate candidates, timezone boundaries and edited/deleted receipts.
- [x] 2.3 Replace fixed-day settings and cycle controls with accessible source, candidate and correction flows; verify keyboard, screen-reader labels, empty and ambiguous states.

## 4. Useful insights and Kakeibo

- [x] 4.1 Retire linear forecasts, the debt payoff-time estimate, and misleading daily allowance and pace judgments across consumers; show observed cash flow, debt progress, budgets and identified commitments with visible periods. Verify irregular spending and unknown payday tests.
- [x] 4.2 Audit and correct five-pillar and fresh-savings aggregation against movements, reimbursements, investment funding, trades and fees; verify individual amounts reconcile to linked ledger rows without double counting.
- [x] 4.3 Present five classifications and explain optional benchmark mapping and the distinction between surplus and savings; verify drill-down, accessible labels, responsive layout and rounding.

## 5. Plans and actual payments

- [x] 5.1 Inventory legacy scheduled execution, payroll, debt and investment paths and migrate existing rules into non-posting expectations without erasing history; verify no scheduler posts after cutover and interrupted migration can safely retry.
- [x] 5.2 Retire payroll bulk posting and dedicated bill-payment shortcuts with explicit compatibility behavior for old API clients; verify existing records remain intact and requests cannot silently create duplicates.
- [x] 5.3 Link recorded actual payments to obligations, flag ambiguous matches and provide explicit manual capture for missed events; verify one-time debt updates and concurrent/repeated actions.
- [x] 5.4 Present and manage expectations separately from actual transactions and update user-facing documentation; verify reminders do not change balances and manual investment and debt workflows remain intact.

## 6. Integrated verification and delivery

- [x] 6.1 Run repository CI-equivalent tests, lint, type checks, builds, `git diff --check` and `openspec validate audit-financial-calculations-and-automation --strict` on the final revision; record pass/fail evidence.
- [x] 6.2 Run synthetic end-to-end scenarios across backend, web and Android including duplicate capture, edits, offline refresh, salary gaps and migration/rollback; verify existing emergency coverage behavior is preserved and record exact revisions without personal amounts. Backend and web scenarios and rollback are done; the Android device run remains (see verification.md).
- [x] 6.3 Review the final diff, compatibility and rollout/rollback instructions, including a guard against silently re-enabling auto-posting; record remaining risks for owner acceptance.

## Workflow follow-up

- Present the completed evidence for owner acceptance before archival.
- Revisit emergency-cost and coverage calculations after another two to three months of use, following the owner's decision on 2026-10-30. Do not assume the history is complete automatically.
