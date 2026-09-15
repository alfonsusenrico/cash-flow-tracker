## 1. Database Migration & Schema

- [x] 1.1 Create migration `db/migrations/V10__recurring_rules.sql` defining `recurring_rules` table with foreign keys to accounts, categories, and obligations.
- [x] 1.2 Add `recurring_rule_id` column to `transactions` table with foreign key to `recurring_rules(id)`.
- [x] 1.3 Apply and verify migration in PostgreSQL database.

## 2. Backend API & Execution Engine

- [x] 2.1 Implement schedule calculation helper in backend (calculating `next_due_date` for `monthly_day`, `payday`, and `weekly`).
- [x] 2.2 Create `app/routers/recurring.py` with CRUD endpoints for recurring rules (`GET`, `POST`, `PUT`, `DELETE`, `toggle`).
- [x] 2.3 Implement execution engine (`POST /api/recurring/execute`) handling both atomic transfers and expenses, updating account balances, and decrementing linked obligations.
- [x] 2.4 Implement pending due rules query (`GET /api/recurring/pending`) and auto-post trigger (`POST /api/recurring/process-due`).
- [x] 2.5 Implement 1-tap payroll batch allocation endpoint (`POST /api/recurring/payroll/execute`).
- [x] 2.6 Register recurring router in `app/main.py`.

## 3. Backend Verification & Unit Tests

- [x] 3.1 Write automated pytest test suite `backend/tests/test_recurring_and_payroll.py` covering rule creation, scheduling, auto-post execution, debt obligation payoff, and payroll batch execution.
- [x] 3.2 Run pytest and ensure 100% test pass rate.

## 4. Frontend Recurring Rules & Scheduled Transactions UI

- [x] 4.1 Add API client methods in `frontend/src/lib/api.ts` for recurring rules and payroll allocation.
- [x] 4.2 Create `frontend/src/components/recurring/RecurringRulesModal.tsx` to view, add, edit, and toggle automated rules (with auto-post vs confirmation options).
- [x] 4.3 Add Pending Due Transactions banner/pill on the Dashboard (`/` and `/ledger`) allowing 1-tap review and confirmation.

## 5. Frontend 1-Tap Payroll Allocation Cockpit

- [x] 5.1 Create `frontend/src/components/recurring/PayrollAllocationModal.tsx` showing the salary breakdown into accounts and pockets.
- [x] 5.2 Add "Alokasikan Gaji" quick-action button in Hero Cockpit on the Dashboard and Accounts page.
- [x] 5.3 Implement atomic batch execution flow with inline nominal tweaks, source selection, and instant balance updates.

## 6. End-to-End Verification & Validation

- [x] 6.1 Run frontend type-check, lint, and production build (`npm run type-check && npm run lint && npm run build`).
- [x] 6.2 Rebuild and restart Docker containers (`ledger_api` and `ledger_frontend`).
- [x] 6.3 Validate OpenSpec change with `openspec validate automated-transactions-and-payroll-allocation`.
- [x] 6.4 Perform end-to-end smoke test on browser (http://localhost:8090).
