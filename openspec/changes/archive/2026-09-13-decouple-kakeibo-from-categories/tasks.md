# Tasks: Decouple Kakeibo from Categories

## 1. Frontend Quick Capture Refinement

- [x] 1.1 In `frontend/src/components/ui/QuickCaptureModal.tsx`, remove the `(kakeibo_type)` suffix from category dropdown options.
- [x] 1.2 In `frontend/src/components/ui/QuickCaptureModal.tsx`, remove the automatic override of `kakeiboType` in `handleCategoryChange`.

## 2. Category Management & Chart Cleanup

- [x] 2.1 In `frontend/src/app/insights/page.tsx`, remove the Kakeibo pillar selector from the Category Create/Edit modal.
- [x] 2.2 In `frontend/src/app/insights/page.tsx`, remove the "Pilar Kakeibo" column from the category budget table.
- [x] 2.3 In `frontend/src/components/dashboard/CategoryDonutChart.tsx`, remove the `Need` / `Want` pill badges from the highlight banner and category breakdown list.

## 3. Backend Kakeibo Aggregation

- [x] 3.1 In `backend/app/routers/dashboard.py`, update `get_kakeibo_breakdown` to aggregate directly from `COALESCE(t.kakeibo_type, 'need')` without category-level fallback.
- [x] 3.2 Run backend pytest suite to verify Kakeibo test cases pass.

## 4. End-to-End Verification & Build

- [x] 4.1 Run frontend type-check, lint, and production build (`npm run type-check && npm run lint && npm run build`).
- [x] 4.2 Rebuild and restart Docker containers (`ledger_api` and `ledger_frontend`).
- [x] 4.3 Validate OpenSpec change with `openspec validate decouple-kakeibo-from-categories`.
