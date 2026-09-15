## 1. Database Schema & Migration

- [x] 1.1 Create migration `db/migrations/V2__personal_finance_os.sql` defining `goals`, `obligations`, and adding `goal_id` / `obligation_id` to `transactions`.
- [x] 1.2 Update backend `db/init.sql` and `app/db/init_db.py` to support fresh installations with the 7-table schema.

## 2. Backend Analytics & Domain Endpoints

- [x] 2.1 Implement `app/routers/goals.py` for CRUD operations on savings targets and contribution pacing.
- [x] 2.2 Implement `app/routers/obligations.py` for CRUD operations on debts and payoff progress calculations.
- [x] 2.3 Extend `app/routers/transactions.py` to handle linking transactions to goals and obligations with automatic balance updates.
- [x] 2.4 Implement `app/routers/dashboard.py` with `/api/dashboard/overview`, `/api/dashboard/analytics`, and `/api/dashboard/net-worth` generating cumulative time-series, burn cadence, and runway data.
- [x] 2.5 Mount new routers in `app/main.py`.
- [x] 2.6 Add comprehensive backend unit tests in `backend/tests/test_personal_finance_os.py` covering goals, obligations, and analytics endpoints.

## 3. Frontend Executive Navigation & Layout

- [x] 3.1 Create Executive TopBar with brand emblem, greeting, timeframe selector (`cycle`, `30d`, `90d`), privacy toggle, and global quick-add modal trigger (`N`).
- [x] 3.2 Update navigation tabs: `Overview` (`/`), `Spending Analytics` (`/insights`), `Accounts & Net Worth` (`/accounts`), `Goals & Debts` (`/goals`).
- [x] 3.3 Create shared chart theme and styling utilities in `frontend/src/components/charts/theme.ts` (custom tooltips, axis formatting, dark/light mode token integration).

## 4. Executive Overview & Visual Charts (Home /)

- [x] 4.1 Build Executive KPI Summary Ribbon (Liquid Net Worth, Inflow, Outflow, Net Savings Rate, Runway Days).
- [x] 4.2 Build Cumulative Cash Flow Area Trendline component (`CumulativeCashFlowChart.tsx`) using Recharts with interactive hover tooltips.
- [x] 4.3 Build Daily Burn Cadence Bar Chart with Safe-to-Spend Reference Line (`BurnCadenceChart.tsx`).
- [x] 4.4 Build Category Spend Donut & Variance Matrix component (`CategoryDonutChart.tsx`).
- [x] 4.5 Assemble the `/` Overview dashboard combining KPI ribbon, trendline, cadence, donut, and quick-access recent ledger feed.

## 5. Deep-Dive Analytics Tab (/insights)

- [x] 5.1 Build Day-of-Week by Week-of-Month spending density Heatmap matrix (`SpendingHeatmap.tsx`).
- [x] 5.2 Build multi-period Category Variance and Budget Progress table with visual progress indicators.
- [x] 5.3 Integrate timeframe selector filtering across the analytics dashboard.

## 6. Accounts, Net Worth & Runway Tab (/accounts)

- [x] 6.1 Build Liquidity Distribution visual chart (Bank vs Cash vs E-wallet breakdown).
- [x] 6.2 Build Net Worth & Cash Runway projection card showing estimated cushion months.
- [x] 6.3 Retain 1-tap account transfer and account card management modal.

## 7. Goals & Obligations Tab (/goals)

- [x] 7.1 Build Savings Goals Tracker view with milestone progress bars, target dates, and monthly contribution pace.
- [x] 7.2 Build Debt & Obligations Payoff Tracker view with remaining balance, payoff percentage, and estimated payoff timeline.
- [x] 7.3 Add interactive modals to create/edit goals and obligations, plus 1-tap deposit/payoff action.

## 8. Verification & Container Deployment

- [x] 8.1 Run backend unit tests (`pytest tests/`) ensuring 100% pass rate.
- [x] 8.2 Run frontend type-check, lint, and production build (`npm run type-check && npm run lint && npm run build`).
- [x] 8.3 Rebuild and restart Docker containers (`docker compose up -d --build`).
- [x] 8.4 Validate OpenSpec change integrity with `openspec validate personal-finance-dashboard-os`.

