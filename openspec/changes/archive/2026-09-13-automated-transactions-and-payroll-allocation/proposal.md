## Why

Users currently have to manually record repetitive monthly transactions (such as debt/loan payments, subscription bills, and recurring living expenses) and individually execute multiple account/pocket transfers whenever their monthly salary arrives. This creates friction, increases forgetfulness, and disrupts accurate daily budgeting and cash flow tracking.

## What Changes

- Add a unified `recurring_rules` database entity to store scheduled and automated transactions (expenses, transfers, incomes).
- Support configurable execution modes per rule: full automatic background recording on due date (`auto_post = true`) or 1-tap review & confirmation banner (`auto_post = false`).
- Support flexible scheduling options: monthly on day X, aligned with user's payday cycle, weekly, or custom intervals.
- Support debt link (`obligation_id`): automatically decrease remaining debt balance on active obligations when recurring debt payments occur.
- Add a dedicated **1-Tap Payroll Allocation** workflow: users can define their salary distribution plan across accounts and pockets, review the breakdown in a split card when payday arrives, and execute all allocations in a single atomic action.
- Add backend API endpoints under `/api/recurring`: CRUD for rules, batch execution, and pending due rule detection.
- Add frontend UI components: "Alokasi Gaji" interactive cockpit modal, scheduled transaction management view/sheet, and pending transaction confirmation banners on the dashboard.

## Capabilities

### New Capabilities
- `automated-transactions`: Unified recurring transaction engine supporting automated expenses, debt payments, account-to-pocket transfers, flexible schedules, and configurable auto-post vs 1-tap confirmation.
- `payroll-allocation-flow`: Dedicated 1-tap salary distribution workflow allowing users to configure, review, tweak, and execute monthly payroll splits into accounts and pockets.

### Modified Capabilities
- `transaction-ledger-management`: Add source tracking for transactions created via recurring rules or payroll allocations (`recurring_rule_id`).
- `obligations-debt-tracker`: Allow automated debt payments to atomically link with obligations and update remaining debt balances.

## Impact

- Database: Migration `V10__recurring_rules.sql` introducing `recurring_rules` table and optional `recurring_rule_id` foreign key on `transactions`.
- Backend: New router `app/routers/recurring.py` and integration into `app/main.py` and pulse/dashboard hooks.
- Frontend: New components in `components/dashboard/` and `/accounts` for scheduled transactions and payroll allocation modal.
