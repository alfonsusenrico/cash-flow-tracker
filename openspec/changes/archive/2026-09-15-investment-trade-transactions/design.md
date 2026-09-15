## Context

Users hold various investment instruments (Stocks in lots/lembar, Mutual Funds in UP, Gold in grams, Crypto in tokens). Over time, users buy and sell units at fluctuating prices. Previously, manual transaction creation only handled generic expense/income/transfer cash movements without updating `units` or recalculating `avg_buy_price` on accounts and pockets.

## Goals / Non-Goals

**Goals:**
- Enable manual transaction entry for investment trades (Buy & Sell) across all instrument types.
- Auto-calculate total nominal IDR from unit quantity $\times$ unit price (with 1 lot = 100 lembar for stocks).
- Accurately accumulate units and recalculate weighted average buy price on purchases.
- Accurately reduce units on sales.
- Ensure trades are treated as asset transfers between the cash funding account and the investment instrument, tagged as `Investasi` (`is_excluded_from_budget = true`).
- Provide an intuitive UI on `/accounts` with a dedicated trade modal and live position/cost impact preview.

**Non-Goals:**
- FIFO / LIFO tax lot tracking with individual lot identifiers — weighted average cost is the standard Indonesian retail investment convention.

## Decisions

### 1. Unified Trade Payload in Transactions Endpoint
- **Decision**: Add `investment_action: str | None`, `units: float | None`, `price_per_unit: float | None` to `TransactionCreate` in `backend/app/routers/transactions.py`.
- **Rationale**: Keeps transaction creation unified in `POST /api/transactions`. A trade is fundamentally a financial transaction that also mutates the balance sheet position of an instrument.

### 2. Weighted Average Cost Recalculation
- **Decision**: When `investment_action == "buy"` and `units > 0` and `price_per_unit > 0`:
  $$\text{new\_units} = \text{old\_units} + \text{units}$$
  $$\text{new\_avg\_price} = \operatorname{round}\left(\frac{(\text{old\_units} \times \text{old\_avg}) + (\text{units} \times \text{price\_per\_unit})}{\text{new\_units}}\right)$$
- **Rationale**: Reflects accurate cost basis across multiple accumulation purchases.

### 3. Dynamic Instrument Inputs in Frontend
- **Decision**: Dynamically render unit labels and hints based on `instrument_type`:
  - `stock`: "Jumlah Lot" (1 lot = 100 lembar) + "Harga Beli per Lembar (IDR)"
  - `mutual_fund`: "Unit Penyertaan (UP)" + "NAB per Unit (IDR)"
  - `gold`: "Berat Emas (Gram)" + "Harga per Gram (IDR)"
  - `crypto`: "Jumlah Koin/Token" + "Harga per Koin (IDR)"
- **Rationale**: Matches the terminology users see on their broker / investment apps.

## Risks / Trade-offs

- **[Risk]** Selling more units than currently held.
  - **Mitigation**: Validate that `units <= current_units` on sell, or clamp units to 0 with a user warning.
