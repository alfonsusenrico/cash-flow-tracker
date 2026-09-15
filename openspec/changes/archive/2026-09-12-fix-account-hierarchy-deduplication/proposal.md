## Why

Linking accounts and pockets to savings goals currently produces incorrect balance calculations due to two related bugs:
1. **Pocket Duplication in Goal Selection:** In the frontend Goal modal, the account list flattens accounts that already contain child pockets, resulting in each child pocket (such as "Dana Darurat") appearing twice and doubling its calculated balance (e.g., Rp 3,500,000 becomes Rp 7,000,000).
2. **Hierarchical Double-Counting:** When both a parent account (whose balance already aggregates its pockets) and its child pockets are selected (e.g., Bibit master account and its mutual fund pockets), the balance sums the parent and children together, tripling the actual balance (e.g., Rp 11,400,615 becomes Rp 34,201,845).

This change establishes canonical deduplication rules across both frontend and backend to guarantee mathematical accuracy.

## What Changes

- **Canonical Hierarchy Deduplication:** An account or pocket contributes to a linked goal's balance if and only if its parent account is NOT also linked (`!a.parent_id || !selected_account_ids.includes(a.parent_id)`).
- **Frontend Goal Modal Account Selection:** Replace buggy `flatMap` array manipulation with an indexed, deduplicated account map. Synchronize parent/child checkbox toggles so checking a parent checks all children, unchecking a child unchecks the parent while keeping other pockets active, and the live balance banner reflects the deduplicated total.
- **Backend Goal Balance Computation:** Update `format_goal_row` in `backend/app/routers/goals.py` to filter out child accounts whose parent is also linked when computing `current_amount` and building `linked_accounts`.
- **Dashboard Ketahanan Dana & Glance Metrics:** Update `calculate_ketahanan_dana` and `get_dashboard_overview` in `backend/app/routers/dashboard.py` to deduplicate linked accounts when calculating emergency fund coverage and goals progress.
- **Pulse Balance Consistency:** Align `backend/app/routers/pulse.py` to use `get_accounts_with_balances` root sum, preventing SQL join multiplication.

## Capabilities

### Modified Capabilities
- `financial-goals-tracker`: Specify hierarchy deduplication rules for account-backed goals so that parent accounts and child pockets linked simultaneously never double-count balances.
- `clean-core-ledger`: Ensure pulse and dashboard account balance calculations adhere to canonical top-level aggregation without cartesian join multiplication.

## Impact

- **Backend:**
  - `backend/app/routers/goals.py`: Deduplicate linked accounts in `format_goal_row`.
  - `backend/app/routers/dashboard.py`: Deduplicate linked accounts in `calculate_ketahanan_dana` and `get_dashboard_overview`.
  - `backend/app/routers/pulse.py`: Use `get_accounts_with_balances` for root account balance sum.
  - `backend/tests/test_personal_finance_os.py`: Add unit tests for parent-child deduplication in goals and ketahanan dana.
- **Frontend:**
  - `frontend/src/app/goals/page.tsx`: Deduplicate account map, fix checkbox synchronization, and use `calculateGoalLinkedBalance`.
  - `frontend/src/app/page.tsx` & `frontend/src/app/accounts/page.tsx`: Remove redundant `flatMap` array operations in transfer modals.
- **Database:** Zero schema modifications.
