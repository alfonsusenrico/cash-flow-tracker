# Proposal

## Why

Financial summaries use inconsistent sources and assumptions that can mislead when notification capture is incomplete, spending is irregular, or salary dates change. The owner wants useful, reconcilable information and an audit of features made redundant by automatic transaction capture.

## What Changes

- Establish shared, explainable ledger reporting for the web and Android companion, with matching date boundaries, source definitions, reconciliation and freshness states.
- Introduce salary cycles based on an owner-confirmed salary source and actual qualifying income, with correction and missing-data handling instead of a fixed payday and invented future cycle end.
- Defer changes to the existing emergency-fund expense basis and coverage display for another two to three months of usage, accepting that the current figures may be inaccurate (owner decision 2026-10-30).
- Remove linear end-of-cycle forecasts and prominent daily-average/pace judgments. Replace unsupported safe-to-spend claims with observed cash flow, configured budgets, and clearly identified upcoming commitments; do not imply a guaranteed balance.
- Remove the debt payoff-time estimate, which does not account for interest or payment changes, while retaining actual debt balances and payoff progress (owner decision 2026-10-30).
- Present the five Kakeibo classifications individually, explain savings and the optional 50/30/20 grouping, and reconcile transfers, reimbursements and transactions to their ledger evidence.
- **BREAKING**: retire automatic recurring ledger posting, payroll allocation execution, and the dedicated bill-payment transaction shortcut in their current forms. Preserve schedules as reminders or expectations when useful, support linking actual recorded transactions to obligations, and retain explicit manual recording for missed notifications.
- Preserve existing financial data and provide a staged, reversible transition for rules and API consumers. Clarify periods and reporting semantics before changing calculations.

## Capabilities

### New Capabilities
- `financial-reporting-consistency`: Shared calculation definitions, provenance, periods, drill-down and freshness across clients.
- `salary-cycles`: Actual confirmed salary events define correctable cycle boundaries.

### Modified Capabilities
- `cycle-end-forecast`: Retire unsupported linear forecasts and associated tiles.
- `finance-dashboard-cockpit`: Replace daily allowance and fixed-day cycle assumptions with observed, explainable information.
- `clean-core-ledger`: Replace fixed-date payroll assumptions while preserving ledger, emergency calculation and balance invariants.
- `automated-transactions`: Transition transaction-generating schedules into non-posting expectations and reminders.
- `payroll-allocation-flow`: Retire payroll-allocation execution without losing prior records.
- `kakeibo-reconciliation`: Show and reconcile all five classifications and distinguish observed savings from cash-flow surplus.
- `reimbursement-offsets`: Preserve category-based offsets while allocating them across effective transaction classifications and removing retired allowance behavior.
- `obligations-debt-tracker`: Replace recurring posting with linking actual payments while retaining debt integrity.
- `tactile-pulse-ui`: Replace the daily allowance and pace status with observed information while preserving quick capture.
- `kazz-ui-and-kakeibo`: Replace the obsolete three-pillar and warning presentation with five classifications.
- `mobile-native-layout`: Update mobile web home actions and Kakeibo presentation while retaining capture and navigation.
- `currency-conversion-and-settings`: Replace fixed payday configuration with the confirmed salary source while preserving other settings.
- `investment-topups`: Keep investment contributions intact while converting the monthly shortcut to a non-posting plan.
- `kakeibo-decoupling`: Preserve effective classification consistently without a retired allowance calculation.
- `transaction-provenance`: Preserve historical recurring and payroll origins while stopping new automated posting.
- `transaction-ledger-management`: Keep historical scheduled origins visible without requiring new scheduled postings.
- `accessible-financial-input-forms`: Preserve eligible account controls for movements and non-posting plans.
- `account-reconciliation`: Preserve excluded adjustment semantics in the new reporting calculations.
- `indonesian-daily-finance-ui`: Correct the net-worth explanation to include assets rather than cash alone.

## Impact

Backend ledger reporting, payroll detection, dashboard, analytics, goals, obligations, schedules and settings; web dashboard, ledger, insights and account views; Android companion home and synchronization contracts. Data migration, job disablement, API versioning/compatibility and rollback must protect historical records. Android is a separate repository and requires a coordinated implementation and delivery plan. No production access or use of real financial records is required for the proposal.
