# Proposal: Portfolio Trade & Kakeibo Reconciliation

## Why
In personal finance and Kakeibo budgeting (the 50/30/20 rule), pillars are designed to track how **active monthly cash flow and income** are distributed among **Needs (50%)**, **Wants (30%)**, and **Savings (20%)**.

Currently, stock trades (buying and selling stocks, mutual funds, or crypto) executed via `InvestmentTradeModal` are logged as transfers and unconditionally marked as `kakeibo_type = 'saving'`. When users trade within their portfolio (such as buying stocks with existing RDN cash, day trading, or rebalancing), the trading turnover accumulates into millions of rupiah in "Savings" (e.g., Rp 6.596.000).

Furthermore, the backend calculated pillar percentages against `total_inflow` (`base_calc = total_inflow if total_inflow > 0 else total_allocated`). When a user has only recorded a small income entry this cycle (e.g., Rp 50.000), dividing by that inflow produces absurd percentages such as **303.2% Need**, **140.8% Want**, and **13,192% Saving**.

## What Changes
1. **Exclude Portfolio Trades from Kakeibo Pillars:**
   - Transactions with `investment_action IN ('buy', 'sell')` or trades between investment/RDN accounts are portfolio asset swaps, not monthly savings from living cash flow. They will have `kakeibo_type = NULL`.
   - Only net fresh capital transfers from daily operating accounts (Bank, Cash, E-Wallet) into Investment/Goal accounts count toward the Savings pillar (`Transfers to Investment - Transfers back to Bank`).
2. **Normalize 50/30/20 Allocation Formula:**
   - Base calculation for Kakeibo allocation percentages switches to `total_allocated = need_spent + want_spent + max(0, net_saving_spent)`.
   - Pillars represent relative share of total allocated funds, ensuring that Need % + Want % + Saving % always sum to 100%.
   - If a monthly budget ceiling exists, expose target quota comparisons cleanly.
3. **Reconcile Existing Database Records:**
   - Update existing trade transactions (e.g. BBCA, BBRI buys totaling Rp 6.596.000) to clear `kakeibo_type` so the dashboard immediately reflects realistic, accurate figures.

## Capabilities
- `<kakeibo-reconciliation>`: Proper separation of portfolio trading turnover from monthly savings, and accurate 50/30/20 allocation percentages.

## Impact
- `backend/app/routers/transactions.py`: Do not auto-assign `kakeibo_type = 'saving'` on investment trades or internal bank transfers.
- `backend/app/routers/dashboard.py`: Exclude trades from saving transfers query; calculate relative pillar percentages from `total_allocated`.
- `frontend/src/components/dashboard/KakeiboPillarCards.tsx`: Render clean relative allocation percentages without overflow distortion.
- `frontend/src/components/dashboard/MetricMatrixGrid.tsx`: Mini Kakeibo bar cleanly distributes across 100% width.
- Database: Clear `kakeibo_type` on existing trade transactions.

## Expected Outcome
- Stock trading volume will no longer inflate the "Tabungan" Kakeibo pillar.
- Kakeibo pillars will show meaningful, logical proportions summing to 100% (e.g., 68% Need, 32% Want, 0% Save).
- The user's dashboard will immediately display clean, realistic numbers without 13,192% anomalies.
