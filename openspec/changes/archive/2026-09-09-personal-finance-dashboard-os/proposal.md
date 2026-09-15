## Why

The current single-column mobile view lacks the comprehensive visual power required for holistic personal financial management. The user needs a true financial management dashboard that combines rich visual charts (cumulative cash flow area trendlines, category breakdown donuts, monthly burn cadence bars with budget benchmarks, cash runway forecasts, day-of-week spending heatmaps, and account liquidity distributions) with a complete personal finance feature set (cash flow, multi-account liquidity, savings/financial goals, debt/obligation tracking, and net worth trendlines) while keeping navigation effortless and clear.

## What Changes

- **Executive Financial Overview**: Responsive modular command center featuring core financial metrics (Liquid Net Worth, Monthly Inflow/Outflow, Safe-to-Spend Runway), cumulative cash flow area trendline, and quick-action hub.
- **Deep-Dive Dashboard Tabs**:
  - **Overview**: Executive financial health, cumulative trendline, spending burn-down, recent ledger.
  - **Spending Analytics**: Donut category breakdown, interactive daily/monthly burn cadence bars, day-of-week and time-of-month heatmap, and budget limit variance.
  - **Accounts & Net Worth**: Asset/account liquidity distribution, liability tracking, and historical/projected net worth trendline.
  - **Goals & Obligations**: Visual savings goals with milestone progress bars and debt/obligation payoff progress meters.
- **Backend Analytics Engine**:
  - Cumulative cash flow time-series aggregation endpoint.
  - Burn rate, cash runway projection, and day-of-week spending heatmap calculations.
  - Entities and endpoints for Financial Goals (`/api/goals`) and Obligations/Debts (`/api/obligations`).
  - Unified Net Worth aggregation endpoint (`/api/analytics/net-worth`).
- **Interactive Visual System**:
  - Integration of lightweight, responsive SVG/Canvas chart components (Area charts, Bar charts, Donut charts, Heatmap matrix) styled with high craft (no AI-slop, clean tabular numbers, high-contrast borders).

## Capabilities

### New Capabilities
- `finance-dashboard-cockpit`: Modular executive dashboard with high-level visual analytics (cumulative cash flow trendlines, burn cadence, account distribution, and runway forecast).
- `financial-goals-tracker`: Savings and milestone target tracking with progress bars, target dates, and monthly contribution pacing.
- `obligations-debt-tracker`: Debt and recurring obligation tracking with payoff progress, interest notes, and payment logging.
- `net-worth-analytics`: Net worth calculation and runway forecasting based on liquid assets, liabilities, and burn rate.

### Modified Capabilities
- `clean-core-ledger`: Extend transaction classification and backend aggregations to support goals, obligations, and rich multi-timeframe analytics (heatmaps, cumulative time-series).
- `tactile-pulse-ui`: Transform layout from mobile single-column neo-bank view into an Executive Overview + Deep Dive Tabs layout with rich multi-chart data visualization.

## Impact

- **Database**: Add lightweight, clean persistence for `goals` (target, current, deadline, priority) and `obligations` (total amount, remaining balance, due date, min payment). Extend transactions with optional goal/obligation link.
- **Backend API**: New routers `/api/goals`, `/api/obligations`, and enhanced `/api/analytics` endpoints for time-series, runway, and heatmap data.
- **Frontend**: Full modular dashboard architecture with Tab navigation (`Overview`, `Analytics`, `Accounts & Net Worth`, `Goals & Debts`), responsive grid layouts, and interactive chart components.
- **Telegram Bot**: Compatible with new unified API endpoints.
