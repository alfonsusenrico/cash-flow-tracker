## Why

The Cash Flow Tracker project evolved from a personal expense logger into an over-engineered, 13-page financial ERP with 34 database migrations, 5 conflicting money abstraction layers (Accounts, Buckets, Allocation Plans, Strategy Rules, Financial Goals, Obligations, Assets, Net Worth), and an austere, spreadsheet-like interface. This cognitive overload and entry friction defeated the app's original mission, causing boredom, guilt, and forgotten entries.

With the project owner's approval for a clean-slate refactor, this change removes the accumulated legacy bloat and rebuilds a lean, tactile personal finance companion optimized for **daily mindfulness, effortless capture, and zero friction**.

## What Changes

- **BREAKING**: Replace 34 legacy database migrations and ~25 database tables with a lean 5-table persistence baseline (`users`, `accounts`, `categories`, `transactions`, `api_keys`).
- **BREAKING**: Permanently decommission over-engineered subsystems: Buckets (virtual envelopes), Allocation Plan state machines, Strategy rule simulators, Goal projections, Debt/Loan Obligations, Asset holdings, and background auto-funding schedulers.
- **BREAKING**: Consolidate backend routing into a unified FastAPI architecture with a single auth dependency supporting both Session Cookies (browser) and Bearer tokens (Telegram bot), eliminating 4,000+ lines of duplicate `web.py` and `public.py` routing.
- Implement high-performance core endpoints:
  - `GET /api/pulse`: Today's spending, daily safe-to-spend allowance, cycle pacing, and today's chronological activity feed.
  - `POST /api/transactions`: 1-call logging for Expense, Income, and Internal Transfers.
  - `GET /api/insights`: Visual category breakdown against monthly targets and daily spending histogram.
  - `GET/POST /api/accounts`: Real-time liquid cash balances and instant transfer movement.
  - `GET/POST /api/categories`: Category management with optional monthly spending limits.
- **BREAKING**: Replace 13 frontend pages with a cohesive, mobile-first **3-Screen Layout**:
  1. **Pulse (Today)**: Glanceable allowance, monthly pace progress, rapid quick-capture bar, and today's timeline.
  2. **Insights (Month)**: Category budget progress bars and cycle spending trends.
  3. **Accounts (Cash Vault)**: Physical bank/cash cards with 1-tap transfer modal.
- Implement the **3-Second Quick Capture Experience**: Thumb-friendly numeric pad, 1-tap recent category chips, smart account defaulting, and instant keyboard shortcuts (`N` for new).
- Redesign the visual system away from cold, sterile grays into a modern, tactile, anti-AI-slop aesthetic (Linear/Wise/Apple Card inspired: crisp Inter typography, tabular numbers, subtle 1px structural borders, emerald/coral/cobalt accents, zero purple neon glow or bubbly empty cards).
- Streamline the Telegram Bot: Retain natural language logging and receipt OCR while pruning bloated tools for deprecated entities.

## Capabilities

### New Capabilities
- `clean-core-ledger`: Lean 5-table schema, liquid account balances, categories with optional monthly limits, payday cycle window calculation, and single-call transaction/transfer recording.
- `tactile-pulse-ui`: Ergonomic 3-view navigation, 3-second rapid transaction capture pad, glanceable daily spending allowance, and an anti-AI-slop visual design system.

### Modified Capabilities
- None. (This change establishes the new clean baseline; legacy un-specced features are intentionally replaced).

## Impact

- **Backend**: Deletes `routers/resources/` (allocation, strategy, buckets, goals, obligations, assets) and consolidates `routers/web.py` and `routers/public.py` into clean, unified route modules. Total backend code reduced by ~70%.
- **Database**: Replaces all legacy migrations with a clean baseline migration (`V1__baseline.sql`). No backward-compatibility migration required per user decision.
- **Frontend**: Overhauls `frontend/src/app/`, reducing routes to `/` (Pulse), `/insights`, and `/accounts`. Updates `tokens.css` and layout components (`AppLayout`, `BottomNav`, `Sidebar`).
- **Telegram Bot**: Retains chat logging and photo receipt extraction; updates endpoint calls to target `/api/transactions` and `/api/accounts`.
- **Dependencies**: Drops unnecessary background scheduling and unused dependencies.

## Expected Outcome

1. **User-Visible**: Opening the app on phone or desktop instantly answers *"How much is safe to spend today?"* and *"Am I on pace for the month?"*. Logging an expense takes fewer than 3 seconds with 1 tap on a category chip. The interface feels fast, tactile, and rewarding rather than like doing corporate accounting.
2. **System-Visible**: Zero background schedulers, zero duplicate routes, 5 clean tables, and sub-15ms response times for all daily endpoints.
