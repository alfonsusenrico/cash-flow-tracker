## 1. Backend Trade Transaction API & Position Recalculation

- [x] 1.1 Extend `TransactionCreate` schema in `backend/app/routers/transactions.py` to accept `investment_action`, `units`, `price_per_unit`
- [x] 1.2 Implement position update logic in `create_transaction` (accumulate units & recalculate weighted average buy price on buy; decrement units on sell)
- [x] 1.3 Add automated unit tests in `backend/tests/test_investment_instruments.py` for manual trade entry and position recalculation

## 2. Frontend Investment Trade Modal & Dynamic Inputs

- [x] 2.1 Create `InvestmentTradeModal.tsx` in `frontend/src/components/ui/` with Buy/Sell toggle, instrument-specific inputs (Stock lots, Gold grams, Mutual Fund UP, Crypto tokens), and live position preview
- [x] 2.2 Add "Beli / Jual Unit" action button to investment account cards and pocket cards on `frontend/src/app/accounts/page.tsx`
- [x] 2.3 Connect `TransactionModal.tsx` in `/ledger` to submit investment trade fields when trading investment instruments

## 3. Verification & Validation

- [x] 3.1 Run full backend test suite (`pytest tests/`)
- [x] 3.2 Run frontend type-check (`npm run type-check`) and build
- [x] 3.3 Rebuild and restart Docker containers (`docker compose up -d --build api frontend`)
- [x] 3.4 Validate change with `openspec validate investment-trade-transactions`
