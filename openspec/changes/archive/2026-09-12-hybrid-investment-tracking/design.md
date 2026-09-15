## Context

The cash flow tracker previously treated all accounts (bank, e-wallet, cash, investment) as static cash reservoirs where balance changes only occurred through user-initiated transactions. In reality, investment instruments (IDX stocks, gold, mutual funds, crypto) experience market value fluctuations, daily closing price movements, and accumulated capital gains or losses.

Users need a hybrid experience:
1. **Dynamic / Automated (Mode A):** Real instruments (e.g. `BBCA.JK`, `GC=F`, `BTC-USD`, `AAPL`) with daily price fetches and automated unit valuation.
2. **Manual / Fallback (Mode B):** Unlisted domestic products (e.g. Indonesian Money Market Funds / Bibit RDPU, deposits) with quick balance updates and capital gain tracking against invested cost.

## Goals / Non-Goals

**Goals:**
- **Zero New Tables:** Maintain the clean 7-table architecture by adding lightweight columns directly to `accounts`.
- **Dynamic Yahoo Finance Search & Quote:** Fast, asynchronous ticker search and quote fetching with zero API keys and zero cost.
- **Unified "Capital Gain" Metric:** Consistent labeling across all UI cards (green for positive, rose for negative).
- **Daily Automated Cron & On-Demand Sync:** Scheduled daily price refresh in FastAPI lifespan + on-demand sync button in the UI.
- **Manual Valuation Modal:** Simple 1-click update flow for untracked mutual funds / deposits.

**Non-Goals:**
- Real-time second-by-second streaming ticker feeds or order book data (daily closing prices are sufficient for personal wealth tracking).
- Portfolio rebalancing engines, dividend reinvestment tax calculations, or multi-currency exchange rate hedging.
- Trade execution or broker API integrations.

## Decisions

### 1. Schema Design: Extend `accounts`
- **Choice:** Add 6 nullable columns to `accounts`:
  - `instrument_type VARCHAR(30) NULL` (`stock`, `mutual_fund`, `gold`, `crypto`, `deposit`, `other`)
  - `instrument_symbol VARCHAR(30) NULL` (e.g. `BBCA.JK`, `GC=F`)
  - `units NUMERIC(18, 6) NULL` (shares, lots $\times 100$, grams, tokens)
  - `avg_buy_price BIGINT NULL` (cost basis per unit in IDR)
  - `last_price BIGINT NULL` (last fetched market price per unit in IDR)
  - `last_price_at TIMESTAMPTZ NULL` (timestamp of last sync)
- **Rationale:** Avoids creating an 8th table or complex asset/holding relational structures while fully satisfying single-stock and single-fund pocket setups (e.g. Master "Bibit" $\rightarrow$ Pocket "BBCA", Pocket "RDPU").

### 2. Market Data Provider: Yahoo Finance Public HTTP Endpoints
- **Endpoints:**
  - Search: `https://query2.finance.yahoo.com/v1/finance/search?q={query}&quotesCount=10&newsCount=0`
  - Quote: `https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d`
- **Implementation:** `app/services/market_data.py` using `httpx.AsyncClient` with custom browser `User-Agent` headers.
- **Fallback / Resilience:** If Yahoo Finance is unreachable or rate-limited, keep previous `last_price` cached in the database.

### 3. Balance & Capital Gain Computation
- **For Ticker-Tracked Accounts with Units:**
  - When `units` and `last_price` exist: $\text{Current Balance} = \text{round}(\text{units} \times \text{last\_price})$.
  - $\text{Capital Gain} = \text{Current Balance} - (\text{units} \times \text{avg\_buy\_price})$.
  - $\text{Capital Gain \%} = \frac{\text{last\_price} - \text{avg\_buy\_price}}{\text{avg\_buy\_price}} \times 100$.
- **For Manual Valuation Accounts:**
  - When user provides updated current balance and initial buy cost:
    - $\text{Capital Gain} = \text{Current Balance} - \text{Initial Cost}$.
    - $\text{Capital Gain \%} = \frac{\text{Current Balance} - \text{Initial Cost}}{\text{Initial Cost}} \times 100$.

### 4. Background Sync Job
- Scheduled background asyncio task in `backend/app/main.py` lifespan running once every 24 hours.
- Queries all active accounts where `instrument_symbol IS NOT NULL`, fetches deduplicated quotes from Yahoo Finance, and updates `last_price`, `last_price_at`, and balances in a single database transaction.

### 5. Frontend UX & Craft
- **Account / Pocket Modal:**
  - When Type = "Rekening Investasi" or pocket is under an investment account:
    - Instrument selector pills (`Saham`, `Reksadana`, `Emas`, `Kripto`, `Deposito`, `Lainnya`).
    - Dynamic search input with debounced lazy-fetch dropdown for tickers.
    - Fields for Units / Lots and Avg Buy Price (or simple Initial Cost if untracked).
- **Accounts Card View:**
  - Displays instrument badge (e.g. `BBCA.JK • Saham`), latest unit price with `last_price_at` relative date ("Hari ini" / "Kemarin").
  - "Capital Gain" badge formatted in green `+Rp ... (+X.X%)` or red `-Rp ... (-X.X%)`.
  - "Update Nilai" button for quick manual valuation on untracked accounts.
  - "Sync Harga Pasar" header button to trigger immediate quote refreshes.

## Risks / Trade-offs

- **[Risk]** Yahoo Finance API might block requests or change response formats.
  - **Mitigation:** Use proper `User-Agent` header, handle timeouts gracefully, cache `last_price` in the database, and allow manual price/balance overrides.
- **[Risk]** Unit precision differences (e.g. Indonesian stocks traded in lots of 100 shares vs crypto fractional units).
  - **Mitigation:** Use `NUMERIC(18, 6)` for `units` to accommodate both whole shares/lots and fractional units, with clear helper text in the frontend ("1 lot = 100 lembar").
