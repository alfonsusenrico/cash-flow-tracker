# Verification: show-daily-spending-at-full-price

- **Branch:** `fix/daily-spending-full-price` (base `eede101a`, working tree ready for commit)
- **Date:** 2026-11-27

## Automated checks

- **OpenSpec strict validation:** `openspec validate --all --strict` — **87 passed, 0 failed**.
- **Removed-symbol reference check:** `grep` across `backend/` and `frontend/` — **0 references** to `daily_offsets`, `offset_day_shares`, `linked_days`, and `linked_by_category_pillar_day`.
- **Backend full test suite (PostgreSQL 16, Flyway v59):**
  `TEST_DATABASE_URL="postgresql://cft:***@127.0.0.1:54399/cft_test_daily" SKIP_TEST_MIGRATIONS=1 REQUIRE_TEST_DATABASE=1 PYTHONPATH=. pytest -q`
  — **2469 passed, 0 failed, 0 skipped** (324.20s).
- **Frontend checks (`frontend/`):**
  - `npm run type-check` — passed.
  - `npm run lint` — passed.
  - `npm test -- --run` — **102/102 test files, 1186/1186 tests passed, 0 skipped**.
  - `npm run build` (`next build --webpack`) — passed.

## Browser & accessibility verification (production build, synthetic data)

- **Target:** Next.js standalone production server + FastAPI backend against a disposable migrated PostgreSQL database seeded with synthetic user `uji_sintetis` (confirmed salary cycles, Rp 1.000.000 synthetic dinner with Rp 600.000 linked reimbursement on a later day).
- **Routes checked:** `/insights` (Ringkasan) and `/insights?tab=kalender` (Kalender).
- **Viewports & themes:** `1440×900` and `390×844` in both `light` and `dark` themes (8 cells total).
- **Results across all 8 cells:**
  - Horizontal overflow: **0** (`scrollWidth === innerWidth` at 1440 and 390 px).
  - Console / runtime errors: **0**.
  - `axe-core` WCAG 2.0/2.1/2.2 A & AA violations: **0 serious/critical, 0 total**.
  - Observable UI: Ringkasan daily chart and Kalender hero rendered `Rp 1.000.000 dibelanjakan · −Rp 600.000 diganti`, peak day `Rp 1.000.000`, and net period hero `Rp 400.000`.

## Not run / caveats

- No production writes, archives, pushes, or pull requests were executed (awaiting owner acceptance).
