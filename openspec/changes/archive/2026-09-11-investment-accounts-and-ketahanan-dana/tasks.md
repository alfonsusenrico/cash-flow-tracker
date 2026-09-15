## 1. Database Schema & Migration

- [x] 1.1 Create migration `db/migrations/V5__emergency_and_primary_flags.sql` adding `is_emergency` to `goals` and `is_primary` to `categories`
- [x] 1.2 Update `db/init.sql` and `backend/app/db/init_db.py` to maintain baseline schema consistency

## 2. Backend API & Ketahanan Dana Formula

- [x] 2.1 Update `AccountCreate` and `AccountUpdate` in `backend/app/routers/accounts.py` to accept `type: "investment"`
- [x] 2.2 Update `GoalCreate` and `GoalUpdate` in `backend/app/routers/goals.py` to accept `is_emergency: bool`
- [x] 2.3 Update `CategoryCreate` and `CategoryUpdate` in `backend/app/routers/categories.py` to accept `is_primary: bool`
- [x] 2.4 Recalibrate `runway` in `backend/app/routers/dashboard.py` to calculate Ketahanan Dana from flagged emergency fund balance and monthly primary living expenses
- [x] 2.5 Add unit tests in `backend/tests/test_personal_finance_os.py` verifying investment account CRUD, emergency goal balance calculation, primary expense filtering, and Ketahanan Dana ratios

## 3. Frontend Investment Account UX

- [x] 3.1 Update account types and interfaces in `frontend/src/app/accounts/page.tsx` and shared domain types to include `"investment"`
- [x] 3.2 Add "Rekening Investasi" option in Add/Edit Account and Pocket modals
- [x] 3.3 Render distinct investment card styling, icon, and 4-way liquidity distribution bar (Bank, E-Wallet, Tunai, Investasi) on `frontend/src/app/accounts/page.tsx`

## 4. Frontend Goals, Categories & Ketahanan Dana Presentation

- [x] 4.1 Add "Tandai sebagai Dana Darurat" toggle in Goal Create/Edit modal on `frontend/src/app/goals/page.tsx`
- [x] 4.2 Add "Pengeluaran Pokok" toggle in Category modals and badges on `frontend/src/app/insights/page.tsx`
- [x] 4.3 Update "Ketahanan Dana" display in `frontend/src/components/dashboard/KpiRibbon.tsx` and `frontend/src/app/accounts/page.tsx` to show $N\times$ monthly primary living costs (e.g. `6.2x Biaya Hidup (6.2 Bulan)`) with status badge

## 5. Verification & Container Rebuild

- [x] 5.1 Run backend pytest suite, frontend type-check, lint, and build
- [x] 5.2 Validate OpenSpec change `openspec validate investment-accounts-and-ketahanan-dana`
- [x] 5.3 Rebuild and restart Docker containers and verify live application
