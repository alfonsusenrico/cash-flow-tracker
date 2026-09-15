## Context

When an account is fresh or has zero balance, the trailing 30-day expense burn rate is 0. Under the previous calculation (`runway_days = round(liquid_net_worth / daily_burn_rate) if daily_burn_rate > 0 else 999`), division by zero was avoided by returning 999 days. However, when `liquid_net_worth <= 0`, a user with zero money was reported as having 999 days (> 1 year, ~32.9 months) of runway in "Aman" status.

## Goals / Non-Goals

**Goals:**
- Correctly report `0 Hari` and `0.0 bulan biaya hidup` when liquid assets are non-positive.
- Distinctly differentiate three runway states:
  1. `liquid_assets <= 0`: 0 Days, status `zero` / `Waspada`, subtitle "Saldo kas masih Rp 0".
  2. `liquid_assets > 0` and `daily_burn == 0`: > 1 Year, status `Aman`, subtitle "Belum ada pengeluaran".
  3. `liquid_assets > 0` and `daily_burn > 0`: standard calculation based on actual burn rate.

**Non-Goals:**
- We are not changing how `daily_burn_rate` itself is queried (30-day trailing sum divided by 30).

## Decisions

### 1. Guarding Zero/Negative Liquid Net Worth
- **Decision**: In `backend/app/routers/dashboard.py`, evaluate `liquid_net_worth <= 0` first:
  ```python
  if liquid_net_worth <= 0:
      runway_days = 0
  elif daily_burn_rate > 0:
      runway_days = round(liquid_net_worth / daily_burn_rate)
  else:
      runway_days = 999
  ```
- **Rationale**: If a user has no funds, runway is zero, period.

### 2. Frontend Adaptive Rendering
- **Decision**: In `KpiRibbon.tsx` and `accounts/page.tsx`, handle zero balance explicitly:
  - If `liquid_net_worth <= 0`: show `0 Hari`, badge `Nol` (or `Waspada`), and subtitle "Saldo kas masih Rp 0".
  - If `daily_burn_rate === 0` but balance is positive: show `> 1 Tahun`, badge `Aman`, and subtitle "Belum ada pengeluaran".
