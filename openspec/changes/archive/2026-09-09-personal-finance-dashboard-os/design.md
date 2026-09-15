## Context

The user requested a complete rework from the logic, purpose, and UI design: transforming from a single-column neo-bank mobile app into a comprehensive, visual **Financial Management Dashboard & Personal Finance OS**.

Key design reference: `/Users/enrico/project/student-enrollment-analysis/src/web-ui/` (Executive overview with KPI summaries, Recharts multi-metric trendlines, unit performance bars, dense analytical sections, and smooth responsive layout).

## Goals / Non-Goals

**Goals:**
- Deliver an **Executive Financial Dashboard** that synthesizes cash flow, liquidity, savings goals, debts, and spending analytics into an effortless, visual interface.
- Rich multi-chart visualization using Recharts:
  - Cumulative Cash Flow Area Trendline (Inflow vs Outflow over time).
  - Monthly / Daily Burn Cadence with budget reference benchmarks.
  - Category Spend Breakdown Donut & variance matrix.
  - Cash Runway & Net Worth trajectory projection.
  - Day-of-Week spending density heatmap.
  - Multi-account liquidity distribution.
- Complete Personal Finance feature set:
  - Multi-account tracking (Cash, Banks, E-wallets).
  - Savings & milestone goals tracking with target dates and pacing.
  - Debt & obligation payoff tracking.
  - Net worth and liquid cash runway calculation.
- Tabbed executive navigation:
  - `Overview` (Executive Cockpit)
  - `Analytics` (Deep spending intelligence & cadence)
  - `Accounts & Net Worth` (Vault & balance trajectory)
  - `Goals & Debts` (Savings targets & debt elimination)
- Maintain sub-millisecond API responses and strict "Anti-AI-Slop" aesthetic (crisp tabular figures, high-contrast borders, no muddy neon gradients).

**Non-Goals:**
- External bank syncing / Plaid API integrations (all data remains self-hosted, manual or bot-logged).
- Complex investment broker integration / stock ticker real-time feeds.
- Bloated multi-tier strategy state machines or unneeded abstraction layers.

## Decisions

### 1. Recharts Visual Chart System
- **Decision:** Leverage existing `recharts` (already installed in `frontend/package.json`) for all visual charts.
- **Rationale:** Highly customizable, supports SVG rendering, custom tooltips, responsive containers, and matches the proven pattern in `student-enrollment-analysis`.
- **Alternatives Considered:** Tremor (brings extra Tailwind dependencies and less flexibility), Chart.js (canvas-based, harder to style with CSS variables).

### 2. Clean Entity Model (7 Tables Total)
- **Decision:** Build upon the clean 5-table core by adding only 2 clean, high-utility entities:
  1. `goals`: `(id, user_id, name, target_amount, current_amount, target_date, color, icon, created_at, updated_at)`
  2. `obligations`: `(id, user_id, name, total_amount, remaining_amount, due_date, minimum_payment, notes, created_at, updated_at)`
  3. Extend `transactions`: add nullable `goal_id` and `obligation_id` foreign keys to track contributions and debt payments directly in the ledger.
- **Rationale:** Gives full Personal Finance OS capabilities without the prior architectural bloat (no 34 migrations or separate allocation engine).
- **Alternatives Considered:** Re-introducing the legacy Phase 2-5 buckets/allocations (rejected: caused previous bloat and confusion).

### 3. Backend High-Performance Aggregations (`/api/dashboard/*`)
- **Decision:** Provide dedicated aggregation endpoints:
  - `GET /api/dashboard/overview`: Returns KPI metrics (Liquid Net Worth, Inflow, Outflow, Savings Rate, Runway), cumulative time-series data for the trendline, category breakdown, and accounts summary in a single fast query.
  - `GET /api/dashboard/analytics`: Returns daily/weekly cadence histogram, day-of-week heatmap matrix, and category budget comparisons.
  - `GET /api/dashboard/net-worth`: Returns historical net worth snapshots and runway projections based on liquid balance and 30-day average burn rate.
  - `GET/POST/PATCH/DELETE /api/goals` and `GET/POST/PATCH/DELETE /api/obligations`.
- **Rationale:** Front-end receives pre-computed, chart-ready time-series payloads with zero client-side calculation lag.

### 4. Executive Bento Cockpit + Deep-Dive Tabs Layout (Copilot/Monarch Aesthetic)
- **Executive Navigation Header:** Top bar with brand emblem (`CashFlow`), user greeting, active timeframe switcher (`Cycle`, `30D`, `90D`), privacy eye toggle, and quick-add action (`+ New` / hotkey `N`).
- **Segmented View Tabs:**
  - `Overview` (Executive Bento Cockpit)
  - `Spending Analytics` (Deep-dive cadence, heatmap & budget matrix)
  - `Accounts & Net Worth` (Vault cards, liquidity & runway projection)
  - `Goals & Debts` (Savings targets & debt payoff workbench)
- **Overview Bento Grid (Desktop 12-col / Responsive Mobile):**
  - **Top Ribbon (12 cols):** 5 KPI Stat Cards (Liquid Net Worth, Inflow, Outflow, Net Savings Rate, Cash Runway).
  - **Hero Stage Left (8 cols):** Interactive Recharts Cumulative Cash Flow Area Chart (inflow vs outflow curve with shaded spread and hover inspection) with toggle to daily burn cadence.
  - **Hero Stage Right (4 cols):** Category Spend Donut chart with center summary and quick category legend.
  - **Mid Bento Row (6 cols + 6 cols):**
    - Left: Active Savings Goals glance with milestone progress bars and monthly pacing.
    - Right: Active Debt & Obligations payoff status with remaining balances.
  - **Bottom Panel (12 cols):** Searchable, filterable transaction statement feed with category chips and quick actions.

## Risks / Trade-offs

- **[Chart Rendering on Mobile]** → Recharts ResponsiveContainer needs fixed aspect ratios or height definitions to prevent collapse. Mitigation: Standardize height utility classes (`h-64 sm:h-80`) with CSS overflow handling.
- **[Cumulative Chart Performance on Large Transaction Sets]** → Querying every single transaction could slow down cumulative calculation. Mitigation: Compute time-series points via SQL `generate_series` date buckets directly in PostgreSQL.
- **[Data Migration on Clean Slate]** → User confirmed clean slate restart. We will baseline a single migration `V2__personal_finance_os.sql` or unified baseline.

## Migration Plan

1. Apply DB schema updates (`goals`, `obligations`, transaction link columns).
2. Implement backend aggregation routers (`/api/dashboard/*`, `/api/goals`, `/api/obligations`).
3. Add backend pytest tests for analytics, goals, and obligations.
4. Implement frontend chart theme, executive KPI ribbon, and 4 tabbed dashboard views.
5. Verify build, lint, and full user flow.
