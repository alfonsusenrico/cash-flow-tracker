## Context

The personal finance application supports multi-instrument investment accounts (`type = 'investment'`), hierarchical pockets (`parent_id`), and automated live price tracking via Yahoo Finance for symbols like `BBRI.JK`.
However, notification ingestion for broker notifications (e.g. Stockbit order matches) previously routed trades directly to the master broker account as unallocated cash, leaving `instrument_symbol`, `units`, and `avg_buy_price` unset on the persistence model.

## Goals / Non-Goals

**Goals:**
- Parse and propagate structured investment trade parameters (`instrument_symbol`, `units`, `price_per_unit`, `action`) from incoming notification payloads.
- Automatically provision a child stock pocket under the broker account (`parent_id = broker_account_id`) if one does not already exist for the ticker.
- Incrementally update `units` and recalculate the weighted average cost basis (`avg_buy_price`) on subsequent buy orders.
- Route transfer legs directly between the funding account (`RDN BCA`) and the stock pocket (`BBRI`), ensuring zero double counting and clean rollup under the parent broker.
- Fetch the initial market quote and enable continuous price syncing via `sync_all_tracked_prices`.
- Backfill the existing 14 lots of BBRI for user `alfonsusenrico`.

**Non-Goals:**
- Complex corporate actions (stock splits, rights issues, reverse splits) — handled manually via valuation/edit modals.
- Real-time order book / level 2 streaming — standard periodic/on-demand Yahoo Finance quote polling is sufficient.

## Decisions

### 1. Indonesian Stock Ticker Normalization (`.JK` Suffix)
- **Decision**: Automatically append `.JK` to 4-letter Indonesian stock tickers extracted from Stockbit (e.g., `BBRI` $\rightarrow$ `BBRI.JK`).
- **Rationale**: Yahoo Finance's global ticker API requires the `.JK` exchange suffix to fetch real-time quotes from the Indonesia Stock Exchange (IDX / BEI).

### 2. Unit & Price Semantics (Lembar vs Lot)
- **Decision**: Store `units` in individual shares (lembar) where $\text{units} = \text{lots} \times 100$, and store `avg_buy_price` in IDR per share.
- **Rationale**: Live market quotes from Yahoo Finance report prices per single share (e.g. Rp 3.320). Computing market value as $\text{units} \times \text{last\_price}$ requires `units` to be in shares. The UI already formats stock units as `lembar` with hint `1 lot = 100 lembar`.

### 3. Weighted Average Cost Recalculation
- **Decision**: On additional buy orders, update the pocket's average buy price using:
  $$\text{new\_avg} = \operatorname{round}\left(\frac{(\text{units}_{\text{old}} \times \text{avg}_{\text{old}}) + (\text{units}_{\text{new}} \times \text{price}_{\text{new}})}{\text{units}_{\text{old}} + \text{units}_{\text{new}}}\right)$$
- **Rationale**: Preserves accurate tax and unrealized P&L calculations over multiple accumulation tranches.

### 4. Direct Child Pocket Transfer Targeting
- **Decision**: Set the transaction's `transfer_target_account_id` (or `account_id` on sell) to the child pocket's ID rather than the parent broker account.
- **Rationale**: In `get_accounts_with_balances`, parent accounts compute:
  $$\text{parent\_balance} = \text{parent\_ledger\_balance} + \sum \text{child\_balance}$$
  Targeting the child pocket keeps `parent_ledger_balance = 0` and allows the child's market valuation to represent the position without duplicate balances.

## Risks / Trade-offs

- **[Risk]** Yahoo Finance API temporary rate limit or outage during ingest.
  - **Mitigation**: Auto-provisioning sets `last_price = price_per_unit` as a safe initial fallback. Background sync updates `last_price` asynchronously when the market feed responds.
- **[Risk]** Sell orders without existing pocket.
  - **Mitigation**: If a sell notification arrives without an existing position, log a warning and fall back to crediting the broker account directly.
