## 1. Core Utilities & Global Currency Config

- [x] 1.1 Update `@/lib/utils.ts` to manage active currency state (`currency`, `usdIdrRate`), provide `setCurrencyConfig`, `convertAmount`, and dynamic `fmtMoney`
- [x] 1.2 Update `@/components/charts/theme.ts` to adapt `formatAxisCurrency` for active currency (USD vs IDR)

## 2. Context & Layout Integration

- [x] 2.1 Update `AppLayout.tsx` to query `/auth/currency/rates`, synchronize `setCurrencyConfig`, and provide `currency`, `usdidrRate`, `fmtMoney`, and `bal` via `AppContext`
- [x] 2.2 Update `SettingsModal.tsx` query invalidations and currency selection to trigger immediate refresh

## 3. Views & Modals Currency Audit

- [x] 3.1 Update `frontend/src/app/goals/page.tsx` to consume context `bal`/`fmtMoney` and ensure all goal cards, obligation cards, and pace badges display converted USD
- [x] 3.2 Update `frontend/src/app/accounts/page.tsx` and `frontend/src/app/ledger/page.tsx` to support converted USD balances
- [x] 3.3 Update `frontend/src/app/page.tsx`, `frontend/src/app/insights/page.tsx`, and dashboard components (`HeroWalletCard`, `MetricMatrixGrid`, `DailyBudgetBar`, `KpiRibbon`, `CategoryDonutChart`, `SpendingHeatmap`, `BurnCadenceChart`, `CumulativeCashFlowChart`)
- [x] 3.4 Clean up redundant hardcoded `"Rp "` prefixes in `RecurringRulesModal.tsx`, `PayrollAllocationModal.tsx`, and `PendingScheduledBanner.tsx`

## 4. Verification & Validation

- [x] 4.1 Validate OpenSpec change integrity with `openspec validate currency-conversion-support`
- [x] 4.2 Run frontend type-check, lint, and production build
- [x] 4.3 Verify in browser that USD balances convert accurately and format as expected
