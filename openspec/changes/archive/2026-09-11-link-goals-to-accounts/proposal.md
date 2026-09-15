## Why

Users frequently distribute their savings for a single purpose across dedicated accounts, pockets, and investment vehicles (for example, an Emergency Fund divided between money market funds, gold, and term deposits).

Currently, creating a goal prompts for an arbitrary "Nominal Awal Terkumpul" (Starting Value). This creates cognitive friction because users cannot easily recall the exact scattered sums, and it creates data discrepancies: entering a starting amount does not connect to or reflect the actual balances in those accounts.

Connecting goals directly to one or multiple dedicated accounts/pockets allows goals to automatically reflect true physical balances without manual math or duplicate entries.

## What Changes

- **Account-Backed Goal Linking (1-to-Many)**:
  - Add a lightweight `goal_accounts` join table linking a goal to one or multiple accounts (`account_id`).
  - When a goal has linked accounts, its `current_amount` is dynamically computed as the sum of the current balances of those linked accounts (`sum(linked_accounts.balance)`).
  - As any linked account receives deposits, transfers, or reconciliation adjustments, the goal progress automatically updates in real time.
- **Goal Modal UX Enhancement**:
  - Replace the manual, confusing "Nominal Awal Terkumpul" input with an intuitive **"Hubungkan ke Rekening / Kantong (Opsional)"** selector.
  - Display available accounts with badges and real-time balance chips (e.g. `[x] RDPU Bibit (Rp 5.000.000)`, `[x] Emas (Rp 5.000.000)`, `[x] Deposito (Rp 5.000.000)`).
  - Dynamically preview total accumulated balance and percentage as accounts are checked/unchecked.
  - For unlinked goals (when no accounts are selected), allow traditional manual progress or deposit logging.
- **Goals Card Presentation**:
  - Display linked account tags on the Goal card so users can see at a glance where their emergency fund is physically stored.

## Capabilities

### Modified Capabilities
- `financial-goals-tracker`: Add requirement for linking goals to single or multiple accounts with dynamic balance aggregation.

## Impact

- **Database**: Add `goal_accounts` table (`goal_id`, `account_id`, `created_at`).
- **Backend**:
  - `backend/app/routers/goals.py`: Support `account_ids: list[UUID]` on create and update; return `linked_accounts` and compute `current_amount` dynamically from linked account balances.
  - `db/init.sql` / migrations: Create `goal_accounts` table.
- **Frontend**:
  - `frontend/src/app/goals/page.tsx`: Interactive multi-account selector in Goal modal, dynamic live balance calculation, and linked account chips on Goal cards.
