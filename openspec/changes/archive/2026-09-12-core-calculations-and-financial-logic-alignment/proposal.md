## Why

Recent usage revealed calculation inaccuracies and UX friction in the core financial engine:
1. Investment valuation updates incorrectly triggered account reconciliation transactions with "Gaji" (salary) income, polluting the ledger, inflating monthly cash inflow, and producing distorted "100% tersimpan" savings rate metrics.
2. The primary balance metric mixed non-spendable investment assets into liquid daily cash ("Siap digunakan kapan saja"), obscuring true everyday purchasing power.
3. Daily spending allowance ("Batas Belanja") defaulted to an opaque hardcoded formula rather than honoring explicit user budgets.
4. "Ketahanan Dana" (runway) assumed a fixed threshold without letting users configure their personalized target multiplier (e.g. 6x vs 8x monthly primary expenses).
5. Category budgets listed income categories in spending tables and lacked custom category creation and tag management.
6. Settings modal contained redundant toggles (display privacy) and temporary integrations, while Goals page displayed an unhelpful aggregated target sum.

## What Changes

- **BREAKING (Logic Correction):** Cease creating income/expense transactions during investment valuation updates. Balances are derived dynamically from units and market prices without polluting the cash ledger. Purge legacy valuation transactions.
- **Asset Separation & Liquid True Balance:** Segregate liquid cash & bank balances from investment portfolios. Add a dedicated summary card showcasing: Liquid Cash (Kas & Bank), Investment Portfolio, and Total Net Balance.
- **Transparent Cash Flow Metrics:** Replace ambiguous "Rasio Tabungan / % Tersimpan" with explicit "Surplus Arus Kas" (Net Cash Flow) and real savings transfer tracking.
- **Configurable Daily Allowance (Batas Belanja):** Enable users to set their overall monthly spending budget to calculate daily allowance transparently, eliminating arbitrary fallbacks.
- **Personalized Ketahanan Dana Multiplier:** Allow users to set their emergency fund multiplier (default: 6x, e.g., 8x) to measure runway against personalized living cost targets.
- **User Category Management:** Support full category CRUD with type (Income/Expense), tag (`Pokok` vs `Opsional`), color, icon, and budget ceiling. Filter expense budget matrices to only expense categories.
- **Settings Modal Streamlining:** Remove "Privasi Tampilan" and Telegram bot cards; add user profile information; add IDR/USD currency selector with daily exchange rates.
- **Goals Page Streamlining:** Remove the confusing aggregated "Target Tabungan" summary card that lumped unrelated goals together.

## Capabilities

### New Capabilities
- `custom-category-management`: Full user creation, editing, tagging (`Pokok` / `Opsional`), and budget ceiling configuration for categories.
- `currency-conversion-and-settings`: Streamlined settings modal with user profile info and curated currency selector (IDR/USD) powered by automated daily rates.

### Modified Capabilities
- `clean-core-ledger`: Discontinue transaction generation on investment valuation updates; enforce neutral categorization on cash reconciliations.
- `finance-dashboard-cockpit`: Separate liquid cash from investment balances, transparently calculate monthly surplus and user-configured daily allowance, and evaluate Ketahanan Dana against user target multipliers.
- `financial-goals-tracker`: Eliminate the aggregated Target Tabungan summary card on the goals view.

## Impact

- **Database:** Optional user settings columns for `emergency_fund_target_multiplier` and `monthly_spending_budget`. Data cleanup for erroneous valuation transactions.
- **Backend APIs:** Updates in `app/routers/accounts.py`, `app/routers/dashboard.py`, `app/routers/categories.py`, and `app/services/market_data.py`.
- **Frontend Views:** Adjustments in `src/app/page.tsx`, `src/app/insights/page.tsx`, `src/app/goals/page.tsx`, `src/components/ui/SettingsModal.tsx`, and KPI cards.
