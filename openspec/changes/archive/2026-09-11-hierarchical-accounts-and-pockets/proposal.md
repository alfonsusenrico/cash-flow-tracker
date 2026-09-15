## Why

Modern personal finance tracking in Indonesia and globally involves institutions offering multiple sub-accounts or pockets (e.g. Bank Jago Kantong, Jenius Flexi/Dream Saver, BCA Tahapan vs Deposito, Bibit portfolios). Currently, all accounts exist as a flat list, causing visual clutter when users have 10+ pockets across multiple institutions, and forcing users to manually calculate total balances per bank during statement reconciliation. 

Supporting a clean two-level Parent-Child hierarchy (Master Accounts -> Pockets / Sub-accounts) solves this by matching the user's real-world banking mental model while maintaining a clutter-free, grouped interface.

## What Changes

- **Self-Referential Account Hierarchy**: Add `parent_id UUID NULL REFERENCES accounts(id) ON DELETE CASCADE` to the `accounts` table.
  - Accounts with `parent_id IS NULL` and no children remain standalone accounts (e.g. Cash, GoPay).
  - Accounts with `parent_id IS NULL` that have children act as Master/Parent accounts (e.g. Bank Jago, BCA, Bibit).
  - Accounts with `parent_id IS NOT NULL` act as Child Pockets / Sub-accounts (e.g. Kantong Utama, Kantong Liburan, Deposito).
- **Aggregated Parent Balances**: Master accounts with child pockets automatically report their aggregated total balance (sum of children balances plus any direct master balance).
- **Accounts Page Experience**:
  - Group child pockets neatly beneath their parent institution card.
  - Show institutional total balance alongside expandable/collapsible pocket breakdowns.
  - Provide intuitive actions to create a new pocket under an existing parent or add a new standalone account.
- **Grouped Transaction Selectors**: Update account selection dropdowns across the application (Quick Capture, Ledger, Transfers, Deposit Modal, Goal Linking) to group pockets under their parent institution using `<optgroup>`.
- **Seamless Transfers**: Enable frictionless transfers between pockets of the same institution or across different institutions using the existing atomic transfer model.

## Capabilities

### New Capabilities
<!-- None needed; this refines existing core ledger structures -->

### Modified Capabilities
- `clean-core-ledger`: Add support for hierarchical parent-child accounts, pocket balance aggregation, and grouped account resolution.

## Impact

- **Database**: Add `parent_id` column with index and foreign key cascade to `accounts` table via migration `V4__account_hierarchy.sql`. Update `db/init.sql` and `backend/app/db/init_db.py`.
- **Backend API**:
  - Update `AccountCreate` and `AccountUpdate` in `backend/app/routers/accounts.py` to accept optional `parent_id: UUID | None`.
  - Update `get_accounts_with_balances` and `list_accounts` to return `parent_id`, `is_parent`, `children: [...]`, and aggregated parent balances.
- **Frontend UI**:
  - Update `frontend/src/app/accounts/page.tsx` with hierarchical card grouping, pocket creation modal, and expandable pocket lists.
  - Update account dropdown selectors across QuickCapture, Ledger, and Goal modals to display grouped `<optgroup>` options.
