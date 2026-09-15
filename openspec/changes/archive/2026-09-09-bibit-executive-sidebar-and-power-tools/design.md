## Context

The Personal Finance OS dashboard recently refactored the data model into a clean 7-table schema with rich Recharts visual components (Cumulative Trendline, Burn Cadence, Spending Heatmap, Category Donut). However, the navigation still relies on top tabs, the canvas is width-constrained, and high-frequency workflows (linking transactions to goals/debts, inline category budgeting, transaction editing, and account reconciliation) require multi-step detours.

Drawing inspiration from the fluid full-width layout of `student-enrollment-analysis` and the goal-oriented simplicity of **Bibit** (Indonesia's premier fintech/investment dashboard), this design establishes a persistent left-hand executive navigation bar, a full-bleed workbench layout, and seamless power tools for daily financial operations.

## Goals / Non-Goals

**Goals:**
- Implement a persistent left-hand sidebar navigation bar (`AppShell.tsx`, `Sidebar.tsx`) with collapsible desktop state and mobile slide-over drawer.
- Expand application workbench layout to full display width (`w-full px-4 sm:px-6 lg:px-8 py-6`) without arbitrary container width caps.
- Integrate Bibit-style hero portfolio action pills (`+ Record`, `⇄ Transfer`, `🎯 New Goal`) and goal card aesthetics.
- Implement transaction editing and ledger search at `/ledger` with `PATCH /api/transactions/{id}` and full balance reconciliation.
- Implement 1-tap account balance reconciliation at `/accounts` with automatic variance calculation and audit transactions.
- Implement inline category budget editing on `/insights` via `PATCH /api/categories/{id}`.
- Extend `QuickCaptureModal` (`N`) to link expenses/transfers directly to savings goals or debt obligations.
- Support historical cycle navigation chevrons (`‹ Prev Cycle` / `Next Cycle ›`) on the dashboard header.

**Non-Goals:**
- We will NOT alter the core 7-table database schema; all capabilities are cleanly supported with existing relational foreign keys.
- We will NOT build multi-currency foreign exchange revaluation or automated bank scraping feeds.
- We will NOT introduce complex multi-tier investment portfolio management beyond cash, bank, and wallet holders.

## Decisions

### 1. Persistent Left Sidebar vs. Top Tabs
- **Decision:** Shift primary navigation from top tabs (`TopBar.tsx`) to a persistent left-hand sidebar (`Sidebar.tsx`), with a collapsible toggle (260px expanded down to 72px icon rail) on desktop and a slide-out drawer on mobile.
- **Rationale:** Mirrors the proven architecture of `student-enrollment-analysis` and executive dashboard standards, freeing vertical header space for page breadcrumbs, historical cycle selectors, and global action buttons.
- **Alternatives Considered:** Retaining top navigation was rejected because horizontal tab real-estate is limited when adding the new `/ledger` view, and full-width layouts look unbalanced with centered top tabs.

### 2. Transaction Editing & Reversible Balance Updates
- **Decision:** Expose `PATCH /api/transactions/{id}` to update transaction properties. In a single database transaction, the backend reverts the previous balance impact on accounts, goals, and obligations, then applies the updated impact.
- **Rationale:** Prevents data drift and eliminates the user annoyance of having to delete and re-create transactions to fix minor typos or reassign categories.

### 3. Account Balance Reconciliation Pattern
- **Decision:** Implement `POST /api/accounts/{id}/reconcile` which accepts `actual_balance: int`. If `actual_balance != current_balance`, the backend computes the difference, updates the account balance, and generates an adjustment transaction ("Balance Adjustment") categorized as "Other Income" (if positive) or "Bills & Utilities / Adjustment" (if negative).
- **Rationale:** Provides real-world alignment with physical bank statements without requiring the user to do mental subtraction.

### 4. Bibit-Inspired Hero Action Pills & Aesthetics
- **Decision:** Incorporate Bibit's minimalist portfolio hero card pattern: high-contrast total net worth with clean pill buttons (`+ Record`, `⇄ Transfer`, `🎯 New Goal`) embedded directly in the overview summary.
- **Rationale:** Reduces click distance for primary financial actions to 1 click from anywhere on the overview screen.

## Risks / Trade-offs

- **[Risk] Wide-Screen Chart Stretching:** Expanding to full display width on ultra-wide monitors (e.g. 4K) might stretch charts excessively.
  - *Mitigation:* Employ responsive grid break-outs (`grid-cols-1 lg:grid-cols-12 xl:grid-cols-12 2xl:grid-cols-12`) with maximum height constraints on charts to preserve aspect ratios and visual density.
- **[Risk] Transaction Edit Concurrency:** Editing an older transaction could alter running balances if not atomic.
  - *Mitigation:* Execute the reversal and update inside a PostgreSQL `SERIALIZABLE` or `READ COMMITTED` explicit transaction block with `FOR UPDATE` lock on the affected account row.
- **[Risk] Mobile Navigation Clutter:** Adding more features could overwhelm mobile screens.
  - *Mitigation:* Retain bottom navigation on mobile devices with quick add, while the full navigation remains accessible via the top-left hamburger drawer.
