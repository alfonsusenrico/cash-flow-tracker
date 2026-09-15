## 1. Database Schema & Migration

- [x] 1.1 Create migration `db/migrations/V4__account_hierarchy.sql` adding `parent_id` with foreign key and index
- [x] 1.2 Update `db/init.sql` and `backend/app/db/init_db.py` to maintain baseline schema consistency

## 2. Backend API & Aggregated Hierarchy Logic

- [x] 2.1 Update `AccountCreate` and `AccountUpdate` schemas in `backend/app/routers/accounts.py` to accept `parent_id: UUID | None` with strict 2-level depth validation
- [x] 2.2 Update `get_accounts_with_balances` and `list_accounts` in `backend/app/routers/accounts.py` to structure parent accounts with children and compute aggregated institutional balances
- [x] 2.3 Update archiving and deletion in `backend/app/routers/accounts.py` to cascade cleanly to child pockets
- [x] 2.4 Add unit tests in `backend/tests/test_personal_finance_os.py` verifying pocket balance aggregation, depth validation, and non-duplicate total balance calculation

## 3. Frontend Accounts Page & Pocket Experience

- [x] 3.1 Update account types and interfaces in `frontend/src/app/accounts/page.tsx` and shared types to support `parent_id`, `is_parent`, and `children`
- [x] 3.2 Add pocket creation and management modal in `frontend/src/app/accounts/page.tsx`
- [x] 3.3 Render institutional master cards with expandable pocket lists, individual balances, and quick pocket actions on `frontend/src/app/accounts/page.tsx`

## 4. Grouped Selectors & Flow Integration

- [x] 4.1 Update account dropdown selectors across QuickCaptureModal, Ledger, Transfer modal, and Goal modals to use grouped `<optgroup>`
- [x] 4.2 Verify inter-pocket and cross-bank transfers between pockets

## 5. Verification & Container Rebuild

- [x] 5.1 Run backend pytest suite, frontend type-check, lint, and build
- [x] 5.2 Rebuild and restart Docker containers and smoke test live hierarchical accounts and pockets
