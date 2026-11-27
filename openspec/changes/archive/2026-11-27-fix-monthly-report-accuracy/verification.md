# Verification: fix-monthly-report-accuracy

- **Branch:** `fix/monthly-report-accuracy` (base `a113d4a6`, working tree ready for commit)
- **Date:** 2026-11-27

## Automated checks

- **OpenSpec strict validation:** `openspec validate --all --strict` — **87 passed, 0 failed**.
- **Backend full test suite (PostgreSQL 16, Flyway v59):**
  `TEST_DATABASE_URL="postgresql://cft:***@127.0.0.1:54399/cft_test" SKIP_TEST_MIGRATIONS=1 REQUIRE_TEST_DATABASE=1 PYTHONPATH=. pytest -q`
  — **2474 passed, 0 failed, 0 skipped** (295.94s), including all 5 new regression tests in `tests/test_monthly_report_accuracy.py`.
- **Frontend checks (`frontend/`):**
  - `npm run type-check` — passed.
  - `npm run lint` — passed.
  - `npm test -- --run` — **102/102 test files, 1186/1186 tests passed, 0 skipped**, including 30/30 tests in `src/app/laporan/page.test.tsx` and `src/copyRegister.test.ts`.
  - `npm run build` (`next build --webpack`) — passed.

## Browser & accessibility verification (production build, synthetic data)

- **Target:** Next.js standalone production server + FastAPI backend against a disposable migrated PostgreSQL database seeded with synthetic user `uji_sintetis` (confirmed salary cycles, reimbursed merchant, and synthetic non-reimbursed merchant `Kedai Kopi Contoh`).
- **Routes checked:** `/laporan` (no `?basis=` query parameter).
- **Viewports & themes:** `1440×900` and `390×844` in both `light` and `dark` themes (4 cells total).
- **Results across all 4 cells:**
  - Horizontal overflow: **0** (`scrollWidth === innerWidth` at 1440 and 390 px).
  - Console / runtime errors: **0**.
  - `axe-core` WCAG 2.0/2.1/2.2 A & AA violations: **0 serious/critical, 0 total**.
  - Observable UI: `/laporan` opened directly on `Siklus gaji` (matching `/reports/monthly/latest`) and displayed `Kedai Kopi Contoh` accurately in Top Places.

## Not run / caveats

- No production writes, archives, pushes, or pull requests were executed (awaiting owner acceptance).
