## 1. Backend Power Endpoints

- [x] 1.1 Implement `PATCH /api/transactions/{id}` in `backend/app/routers/transactions.py` with transactional reversal and re-application of balance changes across accounts, goals, and obligations.
- [x] 1.2 Implement `POST /api/accounts/{id}/reconcile` in `backend/app/routers/accounts.py` for balance adjustment with audit transactions.
- [x] 1.3 Implement `PATCH /api/categories/{id}` in `backend/app/routers/categories.py` for partial updates including `monthly_budget`.
- [x] 1.4 Add `q` (search query), `type`, `account_id`, `category_id`, `limit`, and `offset` filter parameters to `GET /api/transactions`.
- [x] 1.5 Add `cycle_offset` parameter to `GET /api/dashboard/overview` and `GET /api/dashboard/analytics` in `backend/app/routers/dashboard.py` for historical cycle exploration.
- [x] 1.6 Add backend tests in `backend/tests/test_power_tools.py` verifying transaction editing, balance reconciliation, and category patching.

## 2. Executive Sidebar & Full-Width Layout Architecture

- [x] 2.1 Refactor `frontend/src/components/layout/AppShell.tsx` and `Sidebar.tsx` to implement a persistent desktop left-hand sidebar with collapse toggle (260px to 72px) and mobile slide-over drawer.
- [x] 2.2 Refactor `frontend/src/components/layout/TopBar.tsx` to streamline the header into breadcrumbs, historical cycle selector chevrons, privacy toggle, theme toggle, and mobile menu trigger.
- [x] 2.3 Expand `AppLayout.tsx` and all page containers to full fluid width (`w-full px-4 sm:px-6 lg:px-8 py-6`) matching the `student-enrollment-analysis` architecture.
- [x] 2.4 Update navigation links in `Sidebar.tsx` and mobile navigation: `Overview` (`/`), `Transactions` (`/ledger`), `Analytics` (`/insights`), `Vault & Accounts` (`/accounts`), `Goals & Debts` (`/goals`).

## 3. Bibit-Inspired Hero Cockpit & Overview (/)

- [x] 3.1 Refactor Overview hero card with Bibit-style action pills (`+ Record`, `⇄ Transfer`, `🎯 New Goal`) and key figures (nominal and percentage changes).
- [x] 3.2 Add historical cycle navigator (`‹ Prev Cycle` / `Next Cycle ›`) integrated with `cycle_offset` state on the Overview and Analytics screens.

## 4. Dedicated Searchable Transaction Ledger (/ledger)

- [x] 4.1 Build `frontend/src/app/ledger/page.tsx` with live search input, type filter chips (`All`, `Expense`, `Income`, `Transfer`), account filter, and category filter.
- [x] 4.2 Build interactive Transaction Edit & Detail Modal supporting inline updates to amount, date, account, category, notes, and goal/obligation links.

## 5. Quick Capture Goal & Debt Linking (QuickCaptureModal.tsx)

- [x] 5.1 Extend `QuickCaptureModal.tsx` with an optional "Link to Goal or Debt" selector.
- [x] 5.2 Auto-populate category/notes when a goal or obligation is selected, and submit with `goal_id` / `obligation_id`.

## 6. Inline Category Budgeting (/insights)

- [x] 6.1 Add click-to-edit budget popover/modal on the Category Variance table in `frontend/src/app/insights/page.tsx`.
- [x] 6.2 Connect budget editing to `PATCH /api/categories/{id}` and trigger optimistic query invalidation.

## 7. Account Balance Reconciliation (/accounts)

- [x] 7.1 Add "Reconcile Balance" action button to account cards on `frontend/src/app/accounts/page.tsx`.
- [x] 7.2 Build reconciliation modal calculating real-time variance and calling `POST /api/accounts/{id}/reconcile`.

## 8. Verification & Deployment

- [x] 8.1 Run backend unit tests (`pytest tests/`) ensuring 100% pass rate.
- [x] 8.2 Run frontend type-check, lint, and production build (`npm run type-check && npm run lint && npm run build`).
- [x] 8.3 Rebuild and restart Docker containers (`docker compose up -d --build`).
- [x] 8.4 Validate OpenSpec change integrity with `openspec validate bibit-executive-sidebar-and-power-tools`.
