## Context

Recent user feedback uncovered calculation inaccuracies and UX ambiguities in the core financial engine:
1. **Investment valuation updates generated fake transactions:** When updating investment account valuations, the system executed `reconcile_account`, which picked the first income category in the DB ("Gaji") and inserted income transactions (+Rp 31.775), distorting the transaction ledger, monthly inflow, and savings rate.
2. **Liquid cash was conflated with investments:** "Total Saldo" in the hero cockpit bundled illiquid investment portfolios (mutual funds, stocks) under "Siap digunakan kapan saja", misrepresenting daily spendable liquidity.
3. **Daily allowance used arbitrary fallbacks:** When no category budgets were set, "Batas Belanja" calculated `(liquid_net_worth * 0.5) / remaining_days`, producing confusing numbers like Rp 1.023.456/day.
4. **Ketahanan Dana was rigid:** Emergency fund runway had no user-configurable multiplier (e.g. 6x vs 8x monthly primary expense).
5. **Category budget table displayed income categories:** "Gaji" and "Pendapatan Lain" appeared in the expense budget table, and users had no UI to create custom categories or manage `Pokok` vs `Opsional` tags.
6. **Settings & Goals had UX clutter:** "Privasi Tampilan" was redundant with the top bar toggle, Telegram bot card was premature, and the Goals page had an unhelpful aggregated "Target Tabungan" card.

## Goals / Non-Goals

**Goals:**
- Decouple investment valuations from transaction creation completely.
- Purge erroneous valuation transactions from the database.
- Separate Liquid Cash (Kas & Bank) from Investment Portfolios across dashboard KPIs and account views.
- Replace ambiguous "100% tersimpan" with transparent "Surplus Arus Kas" (Net Cash Flow).
- Allow users to configure their overall monthly spending budget to calculate daily allowance transparently.
- Allow users to configure their Ketahanan Dana target multiplier (default: 6x, customizable to 8x, etc.).
- Filter the category budget matrix to strictly expense categories, and provide a full Category Management UI.
- Clean up the Settings modal (remove redundant privacy toggle and Telegram card, add profile info, add IDR/USD conversion via Yahoo Finance).
- Remove the aggregated Target Tabungan summary card on the Goals page.

**Non-Goals:**
- Full multi-currency ledger accounting (users choose a primary display currency with conversion).
- Automated trading execution with brokerages.

## Decisions

### Decision 1: Pure Dynamic Valuation Without Ledger Transactions
- **Approach:** Investment accounts track balance via `units * last_price` or direct account balance. Valuation updates will only update `last_price`, `units`, or `initial_balance` on the `accounts` table. They will **never** call `reconcile_account` or insert rows into `transactions`.
- **Cleanup:** Delete existing transactions where notes match `Penyesuaian Nilai Investasi%`.
- **Reconciliation Safety:** Cash/Bank reconciliations will never default to "Gaji"; they will use a dedicated system adjustment category if ever invoked.

### Decision 2: Distinct Liquid Cash vs. Investment Breakdown
- In backend `dashboard.py` and `accounts.py`:
  - `liquid_balance`: sum of accounts with `type IN ('cash', 'bank', 'ewallet')`.
  - `investment_balance`: sum of accounts with `type = 'investment'` or `instrument_type IS NOT NULL`.
  - `total_balance`: `liquid_balance + investment_balance`.
- In the frontend:
  - Hero cockpit: "Total Saldo Kas & Bank" reflects `liquid_balance` (truly ready for daily spending).
  - Add a dedicated Asset Breakdown Card displaying Liquid Cash vs. Investment Portfolio vs. Total Net Balance.
  - Emergency funds held in investments or pockets are tracked exclusively in "Ketahanan Dana" and dedicated goal cards.

### Decision 3: User-Configured Monthly Spending Budget & Daily Allowance
- Add `monthly_spending_budget BIGINT NULL` in user preferences.
- When set:
  $$\text{Batas Belanja Harian} = \max\left(0, \frac{\text{monthly\_spending\_budget} - \text{Pengeluaran Bulan Ini}}{\text{Sisa Hari Menuju Gajian}}\right)$$
- If not set, check if the sum of category budgets $> 0$. If both are 0, prompt the user to set a budget instead of using an arbitrary 50% net worth fallback.

### Decision 4: User-Configurable Ketahanan Dana Target Multiplier
- Add `emergency_fund_multiplier INT NOT NULL DEFAULT 6` to user preferences.
- Target Emergency Fund $= \text{emergency\_fund\_multiplier} \times \text{Pengeluaran Pokok Bulanan}$.
- Current Coverage $= \frac{\text{Saldo Dana Darurat}}{\text{Pengeluaran Pokok Bulanan}}$.
- Visual representation: e.g. `6.2x / 8.0x Biaya Pokok` with progress percentage and status.

### Decision 5: Dedicated Category Management & Strict Expense Filtering
- Filter the Category Variance table on `/insights` to strictly `kind = 'expense'`.
- Provide a modal or drawer to create, edit, and archive categories:
  - Fields: `name`, `kind` (`expense` or `income`), `is_primary` (`Pokok` vs `Opsional`), `icon`, `color`, `monthly_budget`.

### Decision 6: Settings Modal Streamlining & Live USD/IDR Conversion
- Remove "Privasi Tampilan" checkbox (redundant with top-bar eye toggle).
- Remove Telegram bot integration card for now.
- Add user profile section (username and display name).
- Currency dropdown: IDR and USD.
- Utilize existing Yahoo Finance service (`USDIDR=X`) in `market_data.py` to fetch daily exchange rates and cache them in Redis.

### Decision 7: Goals Page Simplification
- Remove the left aggregated "Target Tabungan" summary card.
- The page directly displays the individual goal cards (Dana Darurat, Tabungan, etc.) alongside the debt payoff summary card.

## Risks / Trade-offs

- **[Risk] Purging valuation transactions alters historical balance:**  
  *Mitigation:* In `accounts.py`, investment balances are derived from `units * last_price`. Purging the fake "Gaji" transactions will fix the ledger and monthly cash flow metrics without affecting the investment account's current balance.
- **[Risk] Currency conversion rate API latency:**  
  *Mitigation:* Cache the USD/IDR rate daily in Redis with an in-memory fallback.
