## 1. Backend Cycle Forecast Calculation

- [x] 1.1 Implement `projected_cycle_outflow` in `backend/app/routers/dashboard.py` extrapolating active cycle outflow by cycle elapsed and total days.
- [x] 1.2 Fix rounding precision drift on `projected_30d_outflow` by computing directly without intermediate integer rounding.
- [x] 1.3 Include cycle metadata (`cycle_elapsed_days`, `cycle_total_days`, `cycle_end_date`) in `burn_rate` dictionary response.

## 2. Frontend Insights Tile & Dynamic Context

- [x] 2.1 Update Insights KPI tile in `frontend/src/app/insights/page.tsx` to render "Proyeksi Akhir Siklus" with `burnRate.projected_cycle_outflow`.
- [x] 2.2 Display dynamic cycle progress subtitle showing the cycle end date and elapsed/total day fraction.

## 3. Verification & Testing

- [x] 3.1 Add automated test cases in `backend/tests/test_kakeibo_and_insights.py` asserting exact cycle forecast calculation and absence of rounding drift.
- [x] 3.2 Run full test suite, frontend type-check, build verification, and update Docker containers.
