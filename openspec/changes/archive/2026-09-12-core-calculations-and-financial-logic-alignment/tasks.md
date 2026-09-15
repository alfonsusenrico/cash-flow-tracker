## 1. Database Schema & Data Sanitation

- [x] 1.1 Create migration `db/migrations/V9__financial_logic_and_user_settings.sql` adding `emergency_fund_multiplier INT NOT NULL DEFAULT 6` and `monthly_spending_budget BIGINT NULL` to `users`
- [x] 1.2 Purge erroneous valuation transactions from `transactions` where notes match `Penyesuaian Nilai Investasi%`
- [x] 1.3 Update `backend/app/db/init_db.py` to reflect new user columns and self-healing schema migrations

## 2. Investment Valuation & Core Ledger Logic

- [x] 2.1 Refactor `update_investment_valuation` in `backend/app/routers/accounts.py` to stop invoking `reconcile_account` and completely prevent transaction creation
- [x] 2.2 Update `reconcile_account` in `backend/app/routers/accounts.py` so cash reconciliations never default to "Gaji" and use a neutral adjustment category
- [x] 2.3 Add unit tests in `backend/tests/test_clean_core.py` verifying that investment valuations update balances dynamically without inserting transactions

## 3. Dashboard Calculations, Asset Breakdown & Metrics

- [x] 3.1 In `backend/app/routers/dashboard.py`, separate `liquid_balance` (Cash, Bank, E-wallet) from `investment_balance` and return both in the overview payload
- [x] 3.2 Update `calculate_ketahanan_dana` to calculate target coverage based on the user's configured `emergency_fund_multiplier`
- [x] 3.3 Update safe-to-spend allowance in `dashboard.py` and `pulse.py` to derive from `monthly_spending_budget` or category budgets without arbitrary fallback calculations
- [x] 3.4 In `frontend/src/app/page.tsx` and `KpiRibbon.tsx`, update "Total Saldo" to show true liquid cash, reframe "Rasio Tabungan" to "Surplus Arus Kas", and add the Asset Allocation Breakdown card

## 4. Category Management & Budget Variance

- [x] 4.1 Filter the category budget matrix on `frontend/src/app/insights/page.tsx` to strictly `kind = 'expense'`
- [x] 4.2 Implement category CRUD endpoints (`POST /api/categories`, `PATCH /api/categories/{id}`, `DELETE /api/categories/{id}`) in `backend/app/routers/categories.py`
- [x] 4.3 Build Category Management UI in `frontend/src/app/insights/page.tsx` allowing users to create custom categories, assign `Pokok` vs `Opsional` tags, pick icons/colors, and set monthly budget limits

## 5. Settings Modal Streamlining, Currency & Goals Cleanup

- [x] 5.1 In `backend/app/services/market_data.py`, implement live daily USD/IDR exchange rate retrieval via Yahoo Finance (`USDIDR=X`) with caching
- [x] 5.2 Update `SettingsModal.tsx`: remove "Privasi Tampilan", remove Telegram bot card, display user profile info (`username`), add IDR/USD currency selector, and add Ketahanan Dana multiplier and monthly budget fields
- [x] 5.3 In `frontend/src/app/goals/page.tsx`, remove the aggregated "Target Tabungan" summary card, highlighting individual goals and debt payoff directly

## 6. Verification & Validation

- [x] 6.1 Run backend pytest test suite to ensure 100% pass rate
- [x] 6.2 Run frontend lint, type-check, and production build
- [x] 6.3 Validate OpenSpec change integrity with `openspec validate core-calculations-and-financial-logic-alignment`
- [x] 6.4 Rebuild and restart Docker containers and verify live behavior
