## Why

When an account has a Rp 0 balance and no recorded transactions, the "Ketahanan Dana" (Cash Runway) metric displays `> 1 Tahun` and `~32.9 bulan biaya hidup` with a status badge of `AMAN`. 

This occurs because when the trailing 30-day burn rate is 0, the backend evaluates `runway_days = round(liquid_net_worth / daily_burn_rate) if daily_burn_rate > 0 else 999`. With `liquid_net_worth = 0`, it defaults to 999 days, causing the frontend to divide 999 by 30.4 and report 32.9 months of runway for an account with zero funds.

## What Changes

- **Backend (`dashboard.py`)**:
  - In `GET /overview`: If `liquid_net_worth <= 0`, set `runway_days = 0`. If `liquid_net_worth > 0` and `daily_burn_rate > 0`, calculate `round(liquid_net_worth / daily_burn_rate)`. If `liquid_net_worth > 0` and `daily_burn_rate == 0`, return `999`.
  - In `GET /net-worth`: If `total_assets <= 0`, set `runway_days = 0`, `runway_months = 0.0`, and `status = "zero"`.
- **Frontend (`KpiRibbon.tsx` & `accounts/page.tsx`)**:
  - In `KpiRibbon.tsx`: When `liquid_net_worth <= 0`, render `0 Hari`, badge `Nol` (or `Waspada`), and subtitle `Saldo kas masih Rp 0`. When `liquid_net_worth > 0` and `daily_burn_rate === 0`, render `> 1 Tahun`, badge `Aman`, and subtitle `Belum ada pengeluaran`.
  - In `accounts/page.tsx`: When `totalAssets <= 0`, render `Ketahanan Dana: 0 Hari (Saldo kas Rp 0)`.

## Capabilities

### Modified Capabilities
- `net-worth-analytics`: Require zero cash runway (`0 Hari` / `0.0 bulan`) and appropriate status indicator when liquid assets are non-positive.

## Impact

- **Backend**: `backend/app/routers/dashboard.py` (runway calculation in `/overview` and `/net-worth`).
- **Frontend**: `frontend/src/components/dashboard/KpiRibbon.tsx` and `frontend/src/app/accounts/page.tsx`.
- **Tests**: `backend/tests/test_personal_finance_os.py` (verify runway is 0 when net worth is 0).
