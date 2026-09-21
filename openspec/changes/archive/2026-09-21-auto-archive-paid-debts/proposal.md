# Proposal: Auto-Archive Paid-Off Debts and Obligations

## Why

When a debt obligation is fully paid off (remaining amount reaches 0), it currently remains visible on `/goals` as an active obligation card with a redundant "Bayar Tagihan" action. Furthermore, its minimum payment continues to be tallied into monthly commitments, and it occupies slots in the dashboard obligations glance. Fully paid-off debts must automatically be archived upon completion and removed from active views, while automatically reactivating if a payment is reversed.

## What Changes

- **Automatic Archival on Payment**: When an expense transaction or recurring rule execution reduces an obligation's `remaining_amount` to `0` (or `<= 0`), the obligation's `is_archived` flag is automatically set to `true`.
- **Automatic Archival on Direct Update**: When an obligation is directly updated via `PATCH /api/obligations/{id}` with `remaining_amount: 0`, the obligation is automatically archived (`is_archived = true`).
- **Automatic Reactivation on Reversal/Deletion**: When a transaction linked to an obligation is deleted or modified in a way that restores `remaining_amount > 0`, the obligation's `is_archived` status is automatically restored to `false`.
- **Active Obligations Filtering Guardrail**: In `GET /api/obligations` (when `include_archived=false`), `GET /api/dashboard/overview` (`monthly_commitments`, `obligations_glance`), and `GET /api/dashboard/net-worth`, obligations with `remaining_amount <= 0` or `is_archived = true` are strictly excluded from active counts, lists, and monthly commitments.
- **Database Cleanup Migration**: A startup migration in `init_db.py` ensures existing database rows with `remaining_amount <= 0 AND is_archived = false` are updated to `is_archived = true`.

## Capabilities

### Modified Capabilities
- `obligations-debt-tracker`: Auto-archive paid-off obligations upon debt amortization, automatically reactivate them if payments are reversed, and exclude paid-off obligations from active views and recurring commitments.

## Impact

- `backend/app/routers/transactions.py`: Amortization logic in `create_transaction`, `update_transaction`, and `delete_transaction` to update `is_archived`.
- `backend/app/routers/recurring.py`: Amortization logic in recurring rule execution to update `is_archived`.
- `backend/app/routers/obligations.py`: Update query filters and patch logic to handle auto-archival and prevent 0-balance debts from appearing in active lists.
- `backend/app/routers/dashboard.py`: Exclude 0-balance debts from `monthly_commitments`, `obligations_glance`, and `net_worth`.
- `backend/app/db/init_db.py`: Startup migration to archive existing 0-balance debts.
- `backend/tests/`: Unit and integration test coverage for payoff auto-archival, reversal reactivations, and dashboard exclusions.
