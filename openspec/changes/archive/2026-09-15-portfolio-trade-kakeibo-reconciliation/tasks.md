## 1. Backend Transaction & Kakeibo Calculation Updates

- [x] 1.1 Update `backend/app/routers/transactions.py` to exclude investment trades and general transfers from auto-assigning `kakeibo_type = 'saving'`
- [x] 1.2 Update `get_kakeibo_breakdown` in `backend/app/routers/dashboard.py` to isolate trade turnover from fresh savings and compute relative share against `total_allocated`

## 2. Database Reconcile Existing Trades

- [x] 2.1 Update existing trade transactions in PostgreSQL to set `kakeibo_type = NULL`

## 3. Frontend & System Verification

- [x] 3.1 Verify `KakeiboPillarCards.tsx` and `MetricMatrixGrid.tsx` display normalized percentages cleanly
- [x] 3.2 Run backend pytest suite (`python -m pytest tests/`)
- [x] 3.3 Run frontend type-check (`npm run type-check`) and production build (`npm run build`)
- [x] 3.4 Rebuild and restart Docker containers (`docker compose up -d --build api frontend`)

## 4. OpenSpec Validation

- [x] 4.1 Validate change with `openspec validate portfolio-trade-kakeibo-reconciliation`
