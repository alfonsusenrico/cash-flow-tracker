## 1. Database Schema Extension

- [x] 1.1 Create migration `db/migrations/V6__investment_instruments.sql` adding `instrument_type`, `instrument_symbol`, `units`, `avg_buy_price`, `last_price`, `last_price_at` to `accounts`
- [x] 1.2 Ensure `db/init.sql` and migration runners execute cleanly on the database

## 2. Backend Market Data Service & Account API

- [x] 2.1 Implement `backend/app/services/market_data.py` with Yahoo Finance lazy-search and quote fetching
- [x] 2.2 Update account models and serializers in `backend/app/routers/accounts.py` with instrument fields and Capital Gain computation
- [x] 2.3 Expose `GET /api/accounts/instruments/search` and `POST /api/accounts/sync-prices` in `backend/app/routers/accounts.py`
- [x] 2.4 Add automated daily background sync job in `backend/app/main.py`
- [x] 2.5 Write automated tests in `backend/tests/test_investment_instruments.py` covering instrument search, quotes, capital gain calculation, and price sync

## 3. Frontend Search, Valuation Modals & Card Display

- [x] 3.1 Update frontend API client in `frontend/src/lib/api.ts` with instrument types, search endpoint, and price sync endpoint
- [x] 3.2 Add instrument selector and debounced ticker autocomplete dropdown to Account & Pocket modals in `frontend/src/app/accounts/page.tsx`
- [x] 3.3 Add "Update Nilai" modal for manual valuation and cost basis tracking on untracked investments
- [x] 3.4 Display instrument badge, unit price, and "Capital Gain" pill (green for gain, rose for loss) on investment account and pocket cards
- [x] 3.5 Add "Sync Harga" button to the Accounts page header

## 4. Verification & Container Health Check

- [x] 4.1 Run backend pytest suite to ensure 100% test pass rate
- [x] 4.2 Run frontend type check, lint, and production build
- [x] 4.3 Verify Docker containers build and run healthy
