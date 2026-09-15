# Technical Design: Investment Accounts & Ketahanan Dana Recalibration

## 1. Technical Strategy

### A. Investment Account Type
- **Persistence:** In PostgreSQL `accounts` table, `type` is `VARCHAR(30)`. No column migration needed, but we will add a database migration `V5__emergency_and_primary_flags.sql` that documents allowed values (`'cash'`, `'bank'`, `'wallet'`, `'investment'`).
- **Validation:** Update Pydantic schemas in `backend/app/routers/accounts.py`:
  ```python
  pattern = "^(cash|bank|wallet|investment)$"
  ```
- **Hierarchy Compatibility:** Master accounts and child pockets both support `type = 'investment'`. For instance, a master account "Bibit" can have child pockets "Bibit RDPU", "Bibit SBN", "Bibit Saham".
- **Frontend Styling:**
  - Card style: Deep indigo/violet gradient (`bg-gradient-to-br from-[#1E1B4B] to-[#0F172A]`) with border `border-indigo-500/20`.
  - Icon: `<Icon name="chart" className="h-4 w-4 text-indigo-400" />`.
  - Badge: `INVESTASI` in uppercase font-mono.
  - Liquidity bar: 4th segment `Investasi: {investPct}%` with indigo color.

---

### B. Flagged Emergency Fund Balance
- **Persistence:** Add `is_emergency BOOLEAN NOT NULL DEFAULT FALSE` to `goals` table.
- **Auto-detection / Default:** If `name ILIKE '%dana darurat%'` or `name ILIKE '%emergency%'`, default `is_emergency = TRUE` during migration or insertion.
- **Balance Aggregation:**
  Since Goals already support linking to multiple accounts and pockets via `goal_accounts` (e.g. Dana Darurat linked to Bibit RDPU, BCA Deposito, and Emas), the goal's `current_amount` is already dynamically computed as:
  $$\text{emergency\_balance} = \sum_{\text{goal} \in \text{emergency goals}} \text{goal.current\_amount}$$
- **Fallback:** If no goal has `is_emergency = TRUE`, search for any goal with `"darurat"` or `"emergency"` in its name. If still none, fallback to total liquid cash with a prompt indicator.

---

### C. Primary Expense & Monthly Living Cost Calculation
- **Persistence:** Add `is_primary BOOLEAN NOT NULL DEFAULT TRUE` to `categories` table.
- **Classification:**
  - Default categories like Food/Groceries, Housing, Bills/Utilities, Transport, Health are flagged `is_primary = TRUE`.
  - Discretionary categories like Entertainment, Shopping, Holidays, Hobbies are flagged `is_primary = FALSE`.
- **Monthly Primary Expense Algorithm:**
  ```python
  # 1. Trailing 30-day primary expenses
  cur.execute(
      """
      SELECT COALESCE(SUM(t.amount), 0) AS primary_spent_30d
      FROM transactions t
      LEFT JOIN categories c ON c.id = t.category_id
      WHERE t.user_id = %s
        AND t.type = 'expense'
        AND t.date >= %s
        AND COALESCE(c.is_primary, TRUE) = TRUE
      """,
      (user_id, thirty_days_ago),
  )
  primary_spent_30d = int(cur.fetchone()["primary_spent_30d"])

  # 2. Primary category budget fallback
  cur.execute(
      """
      SELECT COALESCE(SUM(monthly_budget), 0) AS primary_budget
      FROM categories
      WHERE user_id = %s AND kind = 'expense' AND is_archived = FALSE AND is_primary = TRUE
      """,
      (user_id,),
  )
  primary_budget = int(cur.fetchone()["primary_budget"])

  # 3. Monthly obligations (debts & cicilan)
  cur.execute(
      """
      SELECT COALESCE(SUM(minimum_payment), 0) AS monthly_commitments
      FROM obligations
      WHERE user_id = %s AND is_archived = FALSE
      """,
      (user_id,),
  )
  monthly_obligations = int(cur.fetchone()["monthly_commitments"])

  # Effective monthly primary burn
  monthly_need = primary_spent_30d if primary_spent_30d > 0 else primary_budget
  monthly_primary_expense = monthly_need + monthly_obligations
  ```

---

### D. Ketahanan Dana Metrics Formula & Contract
- **Contract:**
  $$\text{coverage\_ratio} = \frac{\text{emergency\_balance}}{\text{monthly\_primary\_expense}}$$
- **Status Thresholds:**
  - $\text{coverage\_ratio} \ge 6.0$: `"healthy"` (Aman)
  - $3.0 \le \text{coverage\_ratio} < 6.0$: `"moderate"` (Cukup)
  - $\text{coverage\_ratio} < 3.0$: `"critical"` (Waspada)
  - $\text{emergency\_balance} \le 0$: `"zero"` (Nol)
- **API Response:**
  ```json
  "runway": {
    "emergency_fund_balance": 36000000,
    "monthly_primary_expense": 6000000,
    "coverage_months": 6.0,
    "runway_days": 182,
    "status": "healthy",
    "emergency_goal_name": "Dana Darurat",
    "primary_expense_breakdown": {
      "spent_30d": 5000000,
      "obligations": 1000000
    }
  }
  ```
