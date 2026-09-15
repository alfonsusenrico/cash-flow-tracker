## Why

The burn rate card on the Insights page currently presents a misleading and mathematically flawed metric labeled "Proyeksi Pengeluaran 30 Hari". 
1. **Premature Integer Rounding**: The backend previously computed `burn_30d = round(spent_30d / 30)` and then multiplied that rounded value by 30 (`burn_30d * 30`). This produced precision artifacts (e.g. recording Rp 50.000 resulted in Rp 50.010).
2. **Disconnected from Real-World Budgeting**: Multiplying a rolling 30-day average by 30 merely recomputes historical spending rather than forecasting. In real life, users manage money from payday to payday (active cycle). They need an actionable answer to: *"If I maintain my spending pace so far this cycle, how much will I end up spending by the time my next paycheck arrives?"*

## What Changes

- Replace the circular 30-day rolling multiplication with an **Active Payday Cycle Forecast** (`projected_cycle_outflow`).
- Calculate the forecast based on actual cycle velocity:
  $$\text{Forecast} = \text{round}\left(\left(\frac{\text{Cycle Outflow}}{\text{Days Elapsed}}\right) \times \text{Total Cycle Days}\right)$$
- Update the Insights burn rate payload contract in `GET /api/dashboard/overview`:
  - Provide `projected_cycle_outflow` alongside unrounded historical data.
  - Return cycle progress context (`cycle_elapsed_days`, `cycle_total_days`, and cycle end date).
- Update the frontend Insights page:
  - Rename the card from "Proyeksi Pengeluaran 30 Hari" to **"Proyeksi Akhir Siklus"**.
  - Update subtext to dynamically display the cycle end date: *"Estimasi total pengeluaran hingga akhir periode (<Tanggal Akhir>)"*.
  - Show comparisons against monthly budget or income if available.

## Capabilities

### New Capabilities
- `cycle-end-forecast`: Payday cycle-aware month-end spending forecast calculation and dashboard UI presentation.

### Modified Capabilities
<!-- No requirement changes to existing capability contracts -->

## Impact

- **Backend**: `backend/app/routers/dashboard.py` (`get_dashboard_overview`), burn rate calculation and test coverage.
- **Frontend**: `frontend/src/app/insights/page.tsx`, burn rate KPI tiles.
- **Dependencies & DB**: No database migrations or schema alterations required. Pure logic and presentation enhancement.
