# Technical Design: Portfolio Trade & Kakeibo Reconciliation

## 1. Context & Architecture Strategy

Kakeibo and the 50/30/20 framework are operational cash-flow allocation tools for **monthly living budgets**. They answer: *"Of the money I consumed or set aside this month, did I adhere to the 50/30/20 balance?"*

Stock trading (buying and selling stocks, mutual funds, gold, rebalancing) is **balance-sheet capital deployment** within an investment vault. Treating every buy trade as a monthly "saving" creates false savings volume and distorts allocation percentages.

```
OPERATIONAL FLOW (Kakeibo Scope):
[Income / Paycheck] ───► [Checking / Cash]
                           ├── 50% Needs (Food, Bills, Rent)
                           ├── 30% Wants (Dining, Shopping)
                           └── 20% Fresh Savings Injection ──► [Investment Vault]
                                                                     │
PORTFOLIO TRADING (Excluded from Kakeibo):                           ▼
[Stockbit RDN Cash] ◄─── Asset Rebalancing (Buy / Sell) ───► [BBCA / BBRI Stock]
```

## 2. Changes to Transaction Processing

In `backend/app/routers/transactions.py`:
- When `payload.investment_action` is provided (trade execution), set `kakeibo_val = None`.
- For standard transfers (`payload.type == 'transfer'`), do **not** automatically assign `kakeibo_val = 'saving'` simply because `transfer_target_account_id` is present.
- A transfer only receives `kakeibo_type = 'saving'` if:
  1. `payload.kakeibo_type == 'saving'` was explicitly passed, OR
  2. `goal_id` is present (dedicated goal funding).

## 3. Changes to Dashboard Kakeibo Calculation

In `backend/app/routers/dashboard.py` -> `get_kakeibo_breakdown`:
1. **Pillar Expenses:**
   - Sum `need` and `want` expenses as usual from `transactions`.
2. **Net Fresh Savings:**
   - Positive savings:
     - Transfers with `kakeibo_type = 'saving'` or `goal_id IS NOT NULL`.
     - Transfers from operational accounts (type IN `bank`, `cash`, `wallet` AND not an RDN/investment account) to investment accounts, excluding trade notes.
   - Negative savings (withdrawals back to operational):
     - Transfers from investment accounts to operational accounts.
   - `net_saving = max(0, fresh_deposits - withdrawals + saving_expenses)`.
3. **Pillar Proportions (The Base Calculation):**
   - `total_allocated = need_spent + want_spent + net_saving`.
   - If `total_allocated > 0`:
     - `need_pct = round((need_spent / total_allocated) * 100, 1)`
     - `want_pct = round((want_spent / total_allocated) * 100, 1)`
     - `saving_pct = round((net_saving / total_allocated) * 100, 1)`
   - Result: Pillars always sum to 100%, providing an intuitive, normalized relative distribution.

## 4. Existing Database Data Cleanup

Run an update query to cleanse past trade transactions:
```sql
UPDATE transactions
SET kakeibo_type = NULL
WHERE user_id = :user_id
  AND (notes LIKE '%lot @%' OR notes LIKE '%Stockbit: Stockbit%');
```

## 5. Frontend Visual Consistency

In `frontend/src/components/dashboard/KakeiboPillarCards.tsx` and `MetricMatrixGrid.tsx`:
- With normalized percentages summing to 100%, the 3-segment bar in `MetricMatrixGrid` fills exactly 100% of the bar width.
- Progress bars in `KakeiboPillarCards` scale up to 100% max width without visual overflow or broken layouts.
