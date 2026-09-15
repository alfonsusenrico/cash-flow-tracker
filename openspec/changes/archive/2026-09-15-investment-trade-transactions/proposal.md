## Why

The application previously lacked the ability to manually record investment trade transactions (buying or selling units) across investment instruments (Stocks, Mutual Funds, Gold, and Crypto). Users could only enter flat cash amounts without specifying units or prices, which meant that accumulating additional units at different prices (averaging down or averaging up) did not update asset quantities or recalculate weighted average purchase prices.

## What Changes

- **Backend Trade Parameters**: Extend `TransactionCreate` to support `investment_action` (`"buy"` or `"sell"`), `units`, and `price_per_unit`.
- **Automatic Position Accumulation & Weighted Cost Recalculation**: In `POST /api/transactions`, when an investment buy transaction is recorded:
  - Increment units on the investment account or pocket.
  - Automatically recalculate the weighted average buy price:
    $$\text{new\_avg} = \operatorname{round}\left(\frac{(\text{old\_units} \times \text{old\_avg}) + (\text{units} \times \text{price})}{\text{old\_units} + \text{units}}\right)$$
  - Route as a `transfer` between the liquid funding account and the investment instrument under category `Investasi` (`is_excluded_from_budget = true`).
  - Symmetrically handle selling (reducing units while preserving cost basis).
- **Frontend Investment Trade Modal & Quick Actions**:
  - Add a dedicated **"Beli / Jual Unit"** action button directly on each investment card and child pocket card on `/accounts`.
  - Provide dynamic unit and price inputs adapted to the instrument type (Lots for stocks, UP for mutual funds, Grams for gold, Tokens for crypto).
  - Show a real-time live preview of the position change and new average purchase price before submitting.
  - Integrate unit and price calculation in the transaction entry modals.

## Capabilities

### New Capabilities
- `investment-trade-recording`: Manual recording of investment buy/sell transactions with dynamic unit inputs, position accumulation, weighted average cost recalculation, and dual-leg transfer creation across all investment instruments.

### Modified Capabilities
<!-- None -->

## Impact

- **Backend**: `backend/app/routers/transactions.py`
- **Frontend**: `frontend/src/app/accounts/page.tsx`, `frontend/src/components/ui/TransactionModal.tsx`
- **Database**: Direct updates to `units`, `avg_buy_price`, and `transactions` tables.
