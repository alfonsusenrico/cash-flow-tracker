## 1. Backend Runway Calculation Guard

- [x] 1.1 Update `GET /api/dashboard/overview` in `backend/app/routers/dashboard.py` to return `runway_days = 0` when `liquid_net_worth <= 0`
- [x] 1.2 Update `GET /api/dashboard/net-worth` in `backend/app/routers/dashboard.py` to return `runway_days = 0`, `runway_months = 0.0`, and `status = "zero"` when `total_assets <= 0`

## 2. Frontend Adaptive Runway Display

- [x] 2.1 Update `KpiRibbon.tsx` to handle `liquid_net_worth <= 0` (showing `0 Hari`, badge `Nol`, subtitle `Saldo kas masih Rp 0`) and `daily_burn_rate === 0` (showing `Belum ada pengeluaran`)
- [x] 2.2 Update `accounts/page.tsx` net worth card to handle `totalAssets <= 0` without displaying false multi-year runway

## 3. Verification & Live Smoke Test

- [x] 3.1 Update backend unit tests in `test_personal_finance_os.py` to verify zero balance runway
- [x] 3.2 Run frontend type-check, lint, and build
- [x] 3.3 Rebuild docker containers and verify live dashboard displays `0 Hari` when balance is Rp 0
