## 1. Backend Hierarchy Deduplication

- [x] 1.1 Update `format_goal_row` in `backend/app/routers/goals.py` to filter out child accounts if parent is also linked
- [x] 1.2 Update `calculate_ketahanan_dana` and `get_dashboard_overview` in `backend/app/routers/dashboard.py` to deduplicate linked accounts with `acc_parent_map`
- [x] 1.3 Update `get_pulse` in `backend/app/routers/pulse.py` to use `get_accounts_with_balances` root sum
- [x] 1.4 Add unit tests in `backend/tests/test_personal_finance_os.py` verifying single pocket, parent+child, and sibling pocket scenarios

## 2. Frontend Goal Account Selection & Dedup UI

- [x] 2.1 Replace buggy `flatMap` in `frontend/src/app/goals/page.tsx` with `accountsMap` indexed by account ID
- [x] 2.2 Implement `calculateGoalLinkedBalance` in `frontend/src/app/goals/page.tsx` with hierarchy deduplication
- [x] 2.3 Implement synchronized parent/child checkbox toggles (`handleParentToggle`, `handleChildToggle`) in `frontend/src/app/goals/page.tsx`
- [x] 2.4 Update live total linked banner and percentage calculation in `frontend/src/app/goals/page.tsx`
- [x] 2.5 Clean up duplicate `flatMap` calls in transfer modals in `frontend/src/app/page.tsx` and `frontend/src/app/accounts/page.tsx`

## 3. Verification & Validation

- [x] 3.1 Run backend pytest suite with 100% pass rate
- [x] 3.2 Run frontend type-check, lint, and build
- [x] 3.3 Validate OpenSpec change integrity (`openspec validate`)
