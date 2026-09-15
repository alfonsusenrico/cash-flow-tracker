## Why

Investment accounts and child pockets currently treat balances as static cash figures. However, real-world investment assets—such as Indonesian stocks (IDX / `.JK`), gold (`GC=F`), US stocks, crypto, and mutual funds (RDPU/RDPT)—fluctuate dynamically in market price or accumulate capital gains over time. Storing static balances fails to reflect actual net worth, requires tedious manual recalibrations, and lacks visibility into portfolio performance (Capital Gain / Loss).

## What Changes

- **Instrument Classification:** Allow investment accounts and child pockets to define their instrument type (`stock`, `mutual_fund`, `gold`, `crypto`, `deposit`, `other`).
- **Dynamic Ticker Search (Mode A):** Integrate a lazy-fetch / debounced search API powered by Yahoo Finance (supporting IDX stocks e.g. `BBCA.JK`, commodities like gold `GC=F`, global tickers, and crypto) with zero external API key requirements.
- **Automated Price Sync & Capital Gain (Mode A):** Support tracking units/lots and purchase price per unit. Compute real-time balance as $\text{Units} \times \text{Current Price}$ and Capital Gain as $\text{Current Balance} - \text{Total Cost}$. Include a daily automated background task and a manual sync trigger to update closing market prices.
- **Manual Valuation & Capital Gain Fallback (Mode B):** For untracked instruments (such as Indonesian money market funds on Bibit/Bareksa or time deposits), provide an "Update Nilai" modal to easily record updated market balances and calculate Capital Gain against initial invested capital.
- **Unified Capital Gain Metric:** Consistently label returns as "Capital Gain" across cards and modals (rendering in emerald green for positive returns and rose/red for negative returns/losses).
- **Lightweight Schema Extension:** Add lightweight columns to the existing `accounts` table (`instrument_type`, `instrument_symbol`, `units`, `avg_buy_price`, `last_price`, `last_price_at`), maintaining the clean relational core with zero redundant tables.

## Capabilities

### Modified Capabilities
- `clean-core-ledger`: Extend account and pocket models to support investment instrument types, dynamic ticker symbol lookup, automated daily market price synchronization, unit/lot tracking, and Capital Gain calculations for both ticker-tracked and manual investment holdings.

## Impact

- **Database:** New migration `db/migrations/V6__investment_instruments.sql` adding `instrument_type`, `instrument_symbol`, `units`, `avg_buy_price`, `last_price`, `last_price_at` to `accounts`.
- **Backend Services & APIs:**
  - New service `app/services/market_data.py` providing asynchronous Yahoo Finance ticker search and quote fetching with robust error handling and user-agent emulation.
  - Endpoints: `GET /api/accounts/instruments/search?q=...` and `POST /api/accounts/sync-prices`.
  - Background scheduler in FastAPI lifespan to run daily market price updates for ticker-tracked accounts.
  - Pydantic models updated for `AccountCreate`, `AccountUpdate`, and account responses.
- **Frontend:**
  - In `frontend/src/app/accounts/page.tsx`: instrument selector, debounced async search dropdown for tickers, units/buy price inputs, "Update Nilai" modal for manual instruments, and live Capital Gain pill display on account/pocket cards.
  - `frontend/src/lib/api.ts`: API methods for searching instruments and syncing prices.
