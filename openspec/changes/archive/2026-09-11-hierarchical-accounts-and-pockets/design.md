## Context

Users track finances across digital banks (Bank Jago, Jenius, Seabank), traditional banks (BCA, Mandiri), and investment apps (Bibit) where money is compartmentalized into multiple pockets or sub-accounts. Currently, all accounts are stored and presented in a single flat list, leading to UI clutter, long dropdown lists, and lack of institutional balance visibility during bank statement reconciliation.

## Goals / Non-Goals

**Goals:**
- Provide a clean 2-level hierarchy: Master Account (Institution) -> Child Pockets (Sub-accounts).
- Implement self-referencing foreign key (`parent_id`) on the existing `accounts` table without creating a 6th table.
- Dynamically aggregate parent balance as the sum of its child pockets plus any direct balance.
- Prevent double counting in overall Net Worth / liquid balance calculations.
- Display an organized Accounts page with institution cards and expandable pocket breakdown.
- Group transaction dropdowns cleanly with `<optgroup>`.
- Allow frictionless transfers between pockets of the same bank or across different banks.

**Non-Goals:**
- We do not support arbitrary N-tier nesting (strictly max 2 levels: Institution -> Pocket).
- We are not reintroducing virtual allocation envelopes, budgeting state machines, or auto-schedulers.
- Pockets are real ledger accounts holding real money, not virtual projections.

## Decisions

### 1. Self-Referential Schema on `accounts` Table
```sql
ALTER TABLE accounts ADD COLUMN parent_id UUID NULL REFERENCES accounts(id) ON DELETE CASCADE;
CREATE INDEX IF NOT EXISTS idx_accounts_parent ON accounts(parent_id);
```
- **Rationale**: A pocket is fundamentally a ledger account—it has an initial balance, holds transactions, can be reconciled, and participates in transfers. Storing pockets in the same table avoids duplicating transaction foreign keys and ledger queries.
- **Alternative considered**: Separate `pockets` table. Rejected because it would require duplicate transaction schemas (`account_id` vs `pocket_id`), separate transfer endpoints, and complex joins across the codebase.

### 2. Strict 2-Level Depth Limit
- A parent account must have `parent_id IS NULL`.
- A pocket has `parent_id` set to a valid parent account.
- When creating or updating an account with `parent_id`:
  The backend verifies `SELECT parent_id FROM accounts WHERE id = %s`. If the target parent itself has a `parent_id`, the request is rejected with `HTTP 400: Child pockets cannot have sub-pockets`.

### 3. Dynamic Balance Aggregation & Non-Duplication
In `get_accounts_with_balances`:
1. Calculate raw ledger balance for each account/pocket from transactions.
2. Group child pockets under their parent: `parent.children = [child1, child2, ...]`.
3. If an account has children, its reported `balance` is:
   $$\text{balance} = \sum_{\text{child} \in \text{children}} \text{child.balance} + \text{parent.initial\_balance}$$
4. Overall Total Liquid Balance:
   $$\text{Total Balance} = \sum_{a \in \text{accounts}, a.\text{parent\_id} \text{ IS NULL}} a.\text{balance}$$
   This ensures zero double-counting while providing accurate institutional and pocket numbers.

### 4. Grouped Dropdowns & Tactile UI
- **Accounts Page**:
  - Standalone accounts (Cash, GoPay) appear as standard single cards.
  - Master accounts (Bank Jago, BCA) appear as institutional cards showing the total aggregated balance, a "+ Tambah Kantong" quick action, and an expandable list of pockets.
- **Transaction & Transfer Selectors**:
  - Render `<optgroup label="Bank Jago">` with its child pockets (`Kantong Utama`, `Kantong Darurat`).
  - Render standalone accounts directly as top-level `<option>` elements.

## Risks / Trade-offs

- **[Risk]** Double-counting balance in KPI widgets (Pulse, Dashboard, Net Worth).
  - *Mitigation*: All high-level balance queries (`total_balance`, `liquid_net_worth`) sum only top-level accounts (`parent_id IS NULL`), which already include child pocket totals.
- **[Risk]** Orphaned child pockets upon parent archiving or deletion.
  - *Mitigation*: Database foreign key has `ON DELETE CASCADE`. When archiving a parent account via API, the backend updates `is_archived = true` for both the parent and all its child pockets.
