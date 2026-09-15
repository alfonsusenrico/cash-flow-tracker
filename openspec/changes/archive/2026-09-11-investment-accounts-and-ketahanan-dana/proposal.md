# Proposal: Investment Accounts & Real-World Ketahanan Dana Calculation

## Why
1. **Investment Accounts Distinction:** Users hold assets in dedicated investment platforms (e.g. Bibit, Stockbit, Bareksa, Pluang) which are neither commercial banks nor cash e-wallets. The existing 3 account types (`cash`, `bank`, `wallet`) force users to mislabel investment accounts as banks or wallets.
2. **True Ketahanan Dana (Emergency Fund Coverage):** Currently, "Ketahanan Dana" simply divides total cash assets by all trailing 30-day daily expenses indiscriminately. In personal finance and according to the user's requirements, **Ketahanan Dana** represents whether the **flagged emergency fund** is sufficient to cover **$N$ times of the monthly primary expense** ($N = \frac{\text{Emergency Fund Balance}}{\text{Monthly Primary Expense}}$), where primary expenses are expenses recorded in essential/primary categories.

## What Changes
- **Account Type Extension:**
  - Add `'investment'` to allowed account types: `cash`, `bank`, `wallet`, `investment`.
  - Support investment accounts and child pockets (e.g. Bibit master account with "RDPU", "SBN", "Saham" pockets).
  - Update account card styles, icons, and liquidity distribution breakdown on the Accounts page.
- **Emergency Fund Flagging:**
  - Add `is_emergency BOOLEAN NOT NULL DEFAULT FALSE` to `goals` table.
  - Users can flag their "Dana Darurat" goal (or any emergency goal). The emergency fund balance is automatically the aggregated balance of that goal and its linked accounts/pockets.
- **Primary Expense Flagging:**
  - Add `is_primary BOOLEAN NOT NULL DEFAULT TRUE` to `categories` table.
  - Expense categories can be designated as Primary / Kebutuhan Pokok (Food, Housing, Utilities, Healthcare, Transport) vs Discretionary / Lifestyle (Entertainment, Shopping, Hobbies).
- **Recalibrated Ketahanan Dana Logic:**
  - Backend computes `monthly_primary_expense` from trailing 30-day primary expenses plus active monthly obligation commitments.
  - Backend calculates `emergency_coverage_ratio = emergency_balance / monthly_primary_expense`.
  - Frontend displays Ketahanan Dana as $N\times$ monthly living costs (e.g. `6.2x Biaya Hidup (6.2 Bulan)`) with status badge (`Aman` $\ge 6\times$, `Cukup` $3-5.9\times$, `Waspada` $< 3\times$).

## Capabilities
- `clean-core-ledger`: Updated with investment account types, emergency goal flag, primary category flag, and true emergency fund coverage calculation.

## Impact
- **Database:** Migration `V5__emergency_and_primary_flags.sql` adding `is_emergency` to `goals` and `is_primary` to `categories`.
- **Backend:** `accounts.py`, `goals.py`, `categories.py`, `dashboard.py`.
- **Frontend:** `accounts/page.tsx`, `goals/page.tsx`, `insights/page.tsx`, `KpiRibbon.tsx`.
- **API Models:** Updated `AccountCreate`, `AccountUpdate`, `CategoryCreate`, `CategoryUpdate`, `GoalCreate`, `GoalUpdate`, and `DashboardOverview` responses.

## Expected Outcome
- Users can create and manage investment accounts (Bibit, Stockbit) alongside Bank, Wallet, and Cash.
- Users can flag their Dana Darurat goal, which aggregates linked emergency accounts/pockets.
- "Ketahanan Dana" provides a mathematically sound, actionable health metric showing exactly how many months of primary living expenses are covered by the emergency fund.
