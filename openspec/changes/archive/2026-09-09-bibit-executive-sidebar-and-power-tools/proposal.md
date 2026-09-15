## Why

While the Personal Finance OS dashboard provides visual intelligence and accurate calculations, daily tracking still suffers from friction: navigation relies on top tabs, the canvas is constrained rather than utilizing the full display width (as achieved in `student-enrollment-analysis`), quick capture cannot directly link to goals or debts, category budgets cannot be edited in-place, past transactions cannot be easily searched or edited, and account balances cannot be quickly reconciled against real bank statements.

Adopting a persistent left-hand sidebar layout (inspired by `student-enrollment-analysis`), expanding to a fluid full-width workbench, and incorporating the clean, goal-oriented aesthetic and swift action workflows of **Bibit** (Indonesia's premier fintech/investment dashboard) transforms the application into a comprehensive, friction-free daily financial cockpit.

## What Changes

- **Full-Width Executive Sidebar Layout:**
  - Replace top tab navigation with a persistent, collapsible left-hand navigation sidebar (featuring Brand mark, 5 main sections, quick-action trigger, user profile, theme toggle, and privacy toggle).
  - Expand application canvas to a fluid, full-width responsive grid (`w-full px-4 sm:px-6 lg:px-8 py-6`) referencing the `student-enrollment-analysis` architecture.
  - Responsive mobile drawer with smooth slide-over navigation.
- **Bibit-Inspired Visual Polish & Hero Cockpit:**
  - Enhance Overview hero area with Bibit-style portfolio action pills (`+ Record`, `⇄ Transfer`, `🎯 New Goal`).
  - Render asset allocation and liquidity distribution with clear percentage badges and clean color bars.
- **Power Tools for Daily Financial Management:**
  - **Quick Capture Goal/Debt Linking:** Extend `QuickCaptureModal` (`N`) with optional "Link to Goal or Debt" dropdown, seamlessly updating goal/obligation balances upon entry.
  - **Dedicated Transaction Ledger (`/ledger`):** Searchable, filterable transaction table with live note/category search, type/account filters, and full transaction detail/edit modal (`PATCH /api/transactions/{id}`).
  - **Inline Category Budgeting (`/insights`):** Clickable budget limit cells and edit popover to adjust monthly category ceilings directly from the variance matrix (`PATCH /api/categories/{id}`).
  - **Account Balance Reconciliation (`/accounts`):** 1-tap "Reconcile Balance" modal allowing users to enter actual current balance, automatically computing variance and generating an adjustment transaction.
  - **Historical Payday Cycle Navigation:** Chevron controls (`‹ Prev Cycle` / `Next Cycle ›`) on the TopBar / Global Filter stack to explore historical cycles.

## Capabilities

### New Capabilities
- `executive-sidebar-layout`: Left-hand collapsible dashboard navigation bar and full-width fluid workbench layout matching `student-enrollment-analysis`.
- `transaction-ledger-management`: Searchable, filterable transaction table with full transaction editing, pagination, and balance adjustments via `PATCH /api/transactions/{id}`.
- `account-reconciliation`: 1-tap balance reconciliation action on account cards with automatic variance calculation and adjustment entry generation.

### Modified Capabilities
- `finance-dashboard-cockpit`: Adding inline category budget editing on `/insights`, historical payday cycle navigation, and Bibit-style hero action pills.
- `tactile-pulse-ui`: Enhancing `QuickCaptureModal` (`N`) with direct goal and debt obligation linking.

## Impact

- **Backend Endpoints:**
  - `PATCH /api/transactions/{id}`: Update transaction amount, category, account, notes, date, or goal/obligation links with automatic ledger and account balance updates.
  - `POST /api/accounts/{id}/reconcile`: Create an adjustment transaction to sync account balance with real-world balance.
  - `PATCH /api/categories/{id}`: Allow updating `monthly_budget` without full overwrite.
  - `GET /api/transactions`: Add query parameters `q` (search), `account_id`, `category_id`, `type`, `limit`, `offset`.
  - `GET /api/dashboard/overview` & `analytics`: Support historical cycle offset (`cycle_offset=0, -1, -2`).
- **Frontend Components:**
  - `AppShell.tsx` & `Sidebar.tsx`: Persistent collapsible left-hand navigation with full-width main workspace.
  - `TopBar.tsx`: Streamlined contextual header with breadcrumb/title, historical cycle navigator, privacy toggle, and quick capture trigger.
  - `QuickCaptureModal.tsx`: Goal and obligation picker integration.
  - `frontend/src/app/ledger/page.tsx`: New dedicated Ledger & Transaction History view with search, filter, and edit modal.
  - `frontend/src/app/insights/page.tsx`: Category budget edit modal.
  - `frontend/src/app/accounts/page.tsx`: Account reconciliation modal.
- **Database:** No schema changes required; the 7-table relational schema fully supports these operations.
