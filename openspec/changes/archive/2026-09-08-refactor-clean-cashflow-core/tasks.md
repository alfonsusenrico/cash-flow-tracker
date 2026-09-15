## 1. Clean Database & Persistence Baseline

**Expected Result:** A lean 5-table SQLite/Postgres baseline schema initialized, all 34 legacy migrations replaced, and verified with schema inspection.

- [x] 1.1 Replace the 34 legacy migration files in `db/migrations/` with a clean `V1__baseline.sql` defining `users`, `accounts`, `categories`, `transactions`, and `api_keys`.
- [x] 1.2 Update backend database initialization and connection pool in `backend/app/db/` to support the 5-table schema without legacy references.
- [x] 1.3 Add seed data script for standard default categories (Food, Transport, Bills, Shopping, Salary, Transfer) and default accounts.

## 2. Unified Backend API & Auth Consolidation

**Expected Result:** A single auth dependency (session cookie + Bearer token), unified domain routes, all legacy resource routers removed, and automated pytest suite passing.

- [x] 2.1 Implement unified `get_current_user` auth dependency in `backend/app/services/auth.py` supporting both session cookies and Bearer tokens seamlessly.
- [x] 2.2 Implement `/api/accounts` CRUD and dynamic balance derivation in `backend/app/routers/accounts.py`.
- [x] 2.3 Implement `/api/categories` with optional monthly budget limits in `backend/app/routers/categories.py`.
- [x] 2.4 Implement `/api/transactions` supporting expenses, income, and atomic internal transfers in `backend/app/routers/transactions.py`.
- [x] 2.5 Implement `/api/pulse` (today's spending, daily safe-to-spend allowance, cycle pace, and today feed) and `/api/insights` (category breakdown, spending histogram) in `backend/app/routers/pulse.py`.
- [x] 2.6 Decommission legacy routers: remove `routers/web.py`, `routers/public.py`, and `routers/resources/` (allocation, buckets, strategy, goals, obligations, assets), and wire clean routers in `backend/app/main.py`.
- [x] 2.7 Write automated pytest test suite covering auth, account balances, transfers, and pulse calculations.

## 3. Anti-AI-Slop Visual System & Core Layout

**Expected Result:** Clean design tokens (`#FAFAF9` light / `#0F1012` dark, Inter tabular numbers, crisp 1px borders), responsive shell, and bottom navigation bar on mobile.

- [x] 3.1 Overhaul `frontend/tokens.css` and `tailwind.config.js` to establish the Linear/Wise/Apple Card palette, removing all gradients, glow effects, and bloated card radii.
- [x] 3.2 Replace `AppShell.tsx`, `Sidebar.tsx`, and `BottomNav.tsx` with an ergonomic 3-view navigation layout (`Pulse`, `Insights`, `Accounts`) that works seamlessly on desktop and mobile.
- [x] 3.3 Create shared UI primitives (`Card`, `Button`, `Input`, `Badge`, `MoneyInput`) with crisp 8px radii, high contrast, and tactile feedback.

## 4. The 3-Screen Frontend Experience

**Expected Result:** The 3 functional screens (`/` Pulse, `/insights`, `/accounts`) rendering real API data with zero unnecessary routes.

- [x] 4.1 Build the **Pulse (Home)** screen at `frontend/src/app/page.tsx`: Big daily allowance card ("Safe to spend today"), monthly pace bar, and today's activity feed.
- [x] 4.2 Build the **3-Second Quick Capture Pad**: Fast numeric pad with inline arithmetic, 1-tap category chips, account defaulting, and desktop keyboard shortcut (`N` key).
- [x] 4.3 Build the **Insights** screen at `frontend/src/app/insights/page.tsx`: Category spending progress bars against monthly limits, daily spending histogram, and cycle comparison.
- [x] 4.4 Build the **Accounts (Vault)** screen at `frontend/src/app/accounts/page.tsx`: Visual account cards with balances and 1-tap instant transfer modal.
- [x] 4.5 Remove legacy routes in `frontend/src/app/` (`buckets`, `allocation`, `strategy`, `goals`, `obligations`, `assets`, `net-worth`, `periods`, `dashboard`, `ledger`).

## 5. Telegram Bot Streamlining & Verification

**Expected Result:** Telegram bot transactions and OCR log directly into the clean `/api/transactions` endpoint; full test suite passes.

- [x] 5.1 Update Telegram Bot API client to point to the new unified `/api/transactions` and `/api/accounts` endpoints, pruning deprecated tools.
- [x] 5.2 Run automated backend tests (`pytest backend/tests/`), frontend lint/typecheck (`npm run type-check`), and end-to-end smoke verification.
- [x] 5.3 Run `openspec validate refactor-clean-cashflow-core` and present verification proof for user acceptance.

