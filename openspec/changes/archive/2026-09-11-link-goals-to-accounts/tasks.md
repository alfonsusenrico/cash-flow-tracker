## 1. Database Schema & Migration

- [x] 1.1 Create `db/migrations/V3__goal_accounts.sql` and update `db/init.sql` for the `goal_accounts` join table

## 2. Backend API & Dynamic Balance Derivation

- [x] 2.1 Update `GoalCreate` and `GoalUpdate` in `backend/app/routers/goals.py` to accept `account_ids: list[UUID]`
- [x] 2.2 Update `GET /api/goals`, `POST /api/goals`, and `PATCH /api/goals/{id}` to manage mappings and dynamically derive `current_amount` from linked accounts
- [x] 2.3 Add unit tests in `backend/tests/test_personal_finance_os.py` verifying multi-account balance aggregation

## 3. Frontend Multi-Account Goal Experience

- [x] 3.1 Add multi-account checklist with live balance preview in Goal Create/Edit modal in `frontend/src/app/goals/page.tsx`
- [x] 3.2 Display linked account chips and individual balances on Goal cards in `frontend/src/app/goals/page.tsx`

## 4. Verification & Container Rebuild

- [x] 4.1 Run backend tests and frontend type-check, lint, and production build
- [x] 4.2 Rebuild and restart Docker containers and verify live multi-account goal tracking
