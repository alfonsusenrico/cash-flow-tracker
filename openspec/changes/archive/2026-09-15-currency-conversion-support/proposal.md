## Why

When users switch their primary display currency from IDR to USD in Settings, the user profile reflects "USD" in the sidebar, but all monetary amounts across the application (balances, savings goals, debts/obligations, transaction feeds, budgets, and chart indicators) remain displayed as raw, unconverted Indonesian Rupiah values prefixed with "Rp". Values must be converted to USD using the daily exchange rate and formatted with standard US dollar notation ($X,XXX.XX).

## What Changes

- **Global Currency Configuration**: Update `@/lib/utils.ts` to maintain dynamic active currency configuration (`currency`, `usdIdrRate`) and provide `fmtMoney`, `fmtCurrency`, and `convertAmount` utilities that convert base IDR values to USD using the exchange rate when USD is selected.
- **Application Context Currency Binding**: Update `AppLayout` context to fetch `/auth/currency/rates`, bind the user's currency preference and USD/IDR rate to global utils and context values (`currency`, `usdIdrRate`, `bal`), ensuring reactive updates when settings change.
- **Masked Balance Formatting**: Update balance hiding helper `bal` to display appropriate currency masks (`$ ••••••` for USD, `Rp ••••••` for IDR) instead of hardcoded `Rp ••••••`.
- **Pages & Components Audit**: Update Goals & Debts (`/goals`), Accounts (`/accounts`), Dashboard (`/`), Transactions (`/ledger`), Insights (`/insights`), and Recurring modals to use unified `bal`/`fmtMoney` without hardcoded "Rp" prefixes or IDR labels.
- **Chart Axis Adaptation**: Update chart formatting (`formatAxisCurrency`) in `components/charts/theme.ts` to adapt scale formatting ($k / $M for USD vs rb / jt / M for IDR).

## Capabilities

### Modified Capabilities
- `currency-conversion-and-settings`: Implement complete frontend currency conversion and display formatting across all screens and components when USD is selected as the primary currency.

## Impact

- **Frontend**: `frontend/src/lib/utils.ts`, `frontend/src/components/layout/AppLayout.tsx`, `frontend/src/app/goals/page.tsx`, `frontend/src/app/accounts/page.tsx`, `frontend/src/app/ledger/page.tsx`, `frontend/src/app/insights/page.tsx`, `frontend/src/app/page.tsx`, and dashboard/modal components.
- **Backend**: No database schema change required; base amounts remain stored in IDR. Existing `/auth/currency/rates` endpoint provides the live Yahoo Finance exchange rate.
