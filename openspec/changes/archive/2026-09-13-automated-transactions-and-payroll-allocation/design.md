## Context

Users currently record recurring payments (debts, subscriptions, insurance, rent) and salary distributions (moving funds from primary payroll accounts into dedicated spending pockets, emergency funds, and investment accounts) by manually entering transactions one by one each month.

In past legacy phases (Phases 2–5), an attempt at automation used overly complex state machines ("Buckets", "Allocation Plans", and background auto-funding schedulers), which led to bloated code, concurrency bugs, and architectural debt. Those were removed in the clean-slate refactor.

This design introduces a clean, minimalist, high-reliability recurring transaction and payroll allocation engine that strictly adheres to the 5-table core ledger philosophy without introducing state machines or complex fuzzy rules.

## Goals / Non-Goals

**Goals:**
- Provide a unified `recurring_rules` entity supporting expenses, transfers, and incomes.
- Support both **Auto-Post** (silent automatic background recording on due date) and **1-Tap Confirmation** (review banner on dashboard when due).
- Support flexible schedules: monthly on day X, payday-aligned (tracks `users.payday_day`), and weekly.
- Link automated debt payments to `obligations`, automatically reducing remaining debt.
- Provide a dedicated **1-Tap Payroll Allocation Modal** where users can review and execute their salary split across accounts/pockets in one click when payday arrives.
- Maintain strict database atomicity and idempotency to prevent duplicate postings.

**Non-Goals:**
- Building an autonomous AI budgeting agent that dynamically reshuffles money without user control.
- Reintroducing legacy multi-layered bucket state machines or auto-funding schedulers.
- External banking API / Open Banking direct debit integrations (this is a self-hosted cash flow ledger).

## Decisions

### 1. Unified `recurring_rules` Schema over Multi-Model Abstraction
- **Decision:** Create a single table `recurring_rules` instead of separate tables for "Recurring Expenses", "Standing Orders", and "Payroll Plans".
- **Rationale:** A transfer from BCA to Jago is structurally identical to an automated subscription payment, except it specifies `target_account_id` instead of `category_id`. Unifying them under one clean schema minimizes complexity and makes rule management straightforward.
- **Alternatives Considered:**
  - *Separate `payroll_plans` table:* Rejected because payroll allocations are simply recurring transfers tagged with `is_payroll_allocation = true`.

### 2. Hybrid Execution: Auto-Post vs. 1-Tap Review Confirmation
- **Decision:** Allow each rule to configure `auto_post: boolean`.
  - If `auto_post = true`: The system executes the transaction on or after `next_due_date` silently and records it in the ledger.
  - If `auto_post = false`: The rule enters a "Due for Review" state, appearing in a sleek notification pill on the dashboard or pulse screen for 1-tap confirmation.
- **Rationale:** Aligns with user preference. Critical fixed payments (like Spotify or KPR) can be fully automated, while variable or discretionary pocket transfers can be verified before posting.

### 3. Dedicated 1-Tap Payroll Allocation Cockpit
- **Decision:** Provide an interactive modal accessible from the Dashboard or Accounts page when payday is near/arrived.
- **Rationale:** Instead of making the user manually record 5–8 separate transfers after receiving salary, the modal displays the entire allocation plan (e.g. BCA -> Makan, BCA -> Transport, BCA -> Bibit), allows quick adjustment of nominals, and commits all movements in a single atomic database transaction.

### 4. Idempotent Execution Engine
- **Decision:** Every execution generates a transaction referencing `recurring_rule_id`, checks that `next_due_date` has not already been fulfilled for the period, and advances `next_due_date` in the same database transaction.
- **Rationale:** Prevents duplicate postings if multiple requests or page refreshes happen concurrently.

## Risks / Trade-offs

- **[Risk] Missed Auto-Post if user doesn't open the app on the exact due date**
  - *Mitigation:* The execution engine checks `next_due_date <= CURRENT_DATE`. If the user opens the app 2 days late, the system immediately catches up and records the transaction on its scheduled date.
- **[Risk] Account Overdraft / Insufficient Balance**
  - *Mitigation:* The system allows transfers to proceed (or warns in the 1-tap confirmation view), preserving user authority over their financial tracking without blocking or aborting mid-cycle.

## Migration Plan

1. Create migration `db/migrations/V10__recurring_rules.sql`.
2. Add `app/routers/recurring.py` and register with FastAPI in `app/main.py`.
3. Add frontend services in `lib/api.ts` and components `PayrollAllocationModal.tsx` and `RecurringRulesSheet.tsx`.
4. Validate with automated unit tests and end-to-end integration tests.
