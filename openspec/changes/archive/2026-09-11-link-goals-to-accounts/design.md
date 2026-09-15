## Context

Users manage funds for savings goals (especially emergency funds) across multiple distinct asset types and accounts (e.g. money market mutual funds, gold holdings, and term deposits). When creating a goal, users were prompted for a manual "Nominal Awal Terkumpul", forcing them to memorize and calculate balances across different accounts and creating a disconnected number not backed by ledger accounts.

## Goals / Non-Goals

**Goals:**
- Allow a goal to be linked to one or more physical accounts or pockets.
- Automatically aggregate `current_amount` from the live balances of linked accounts without manual math.
- Provide a clean, tactile account-selection UI in the Goal modal with real-time balance previews.
- Ensure ongoing changes to account balances (via transfers, deposits, or reconciliations) automatically update goal progress.
- Maintain backward compatibility for standalone unlinked goals.

**Non-Goals:**
- We are not reintroducing the bloated Phase 2/3 virtual bucket system, allocation plans, or auto-schedulers.
- We do not restrict an account to only one goal unless desired by the user.

## Decisions

### 1. Simple Join Table: `goal_accounts`
```sql
CREATE TABLE IF NOT EXISTS goal_accounts (
    goal_id UUID NOT NULL REFERENCES goals(id) ON DELETE CASCADE,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (goal_id, account_id)
);
CREATE INDEX IF NOT EXISTS idx_goal_accounts_goal ON goal_accounts(goal_id);
CREATE INDEX IF NOT EXISTS idx_goal_accounts_account ON goal_accounts(account_id);
```
- **Rationale**: Clean relational model. Easily supports zero, one, or multiple accounts per goal with cascade deletions.

### 2. Dynamic Progress Derivation
- When `list_goals` is called:
  - Query all accounts and their computed live balances using `get_accounts_with_balances(user_id)`.
  - For goals with linked accounts, dynamically calculate `current_amount = sum(a["balance"] for a in linked_accounts)`.
  - Return `linked_accounts: [{id, name, type, balance}, ...]` along with each goal.

### 3. Modal UX: Live Account Balance Aggregation
- In `frontend/src/app/goals/page.tsx`:
  - Show a list of accounts with checkboxes.
  - When accounts are selected, display a live summary banner showing:
    `Total Saldo Terhubung: Rp {sum} ({percentage}% dari target)`.
  - Hide the manual starting value field whenever at least one account is linked.
  - On the goal card, render subtle linked account chips showing where the money is kept.

## Risks / Trade-offs

- **Negative Account Balances**: If an account has an overdraft/negative balance, should it reduce the goal?
  - *Mitigation*: Use `Math.max(0, balance)` or actual balance. By default, savings/investment accounts are non-negative, but using actual balance guarantees accuracy.
