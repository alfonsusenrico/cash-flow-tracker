# Verification: apply-unlinked-reimbursements-to-total

- **Branch:** `feat/apply-unlinked-reimbursements` (base `44640c6`, working tree ready for commit)
- **Date:** 2026-11-27

## Automated checks

- **OpenSpec strict validation:** `openspec validate --all --strict` — **87 passed, 0 failed**.
- **Backend full test suite (PostgreSQL 16, Flyway v59):**
  `TEST_DATABASE_URL="postgresql://cft:***@127.0.0.1:54399/cft_test" SKIP_TEST_MIGRATIONS=1 REQUIRE_TEST_DATABASE=1 PYTHONPATH=. pytest -q`
  — **2474 passed, 0 failed, 0 skipped** (270.64s), including all 5 new regression tests in `tests/test_unlinked_reimbursements.py` and updated reimbursement/top-category contract tests.
- **Frontend checks (`frontend/`):**
  - `npm run type-check` — passed.
  - `npm run lint` — passed.
  - `npm test -- --run` — **102/102 test files, 1186/1186 tests passed, 0 skipped**, including `SpendingTable` and `KakeiboRows` unlinked-reimbursement tests and `copyRegister.test.ts`.
  - `npm run build` (`next build --webpack`) — passed.

## Browser & accessibility verification (production build, synthetic data)

- **Target:** Next.js standalone production server + FastAPI backend against a disposable migrated PostgreSQL database seeded with synthetic user `uji_sintetis` (confirmed salary cycles plus unlinked untagged reimbursements of Rp 150.000 in the closed cycle and Rp 125.000 in the running cycle).
- **Routes checked:** `/laporan?basis=cycle` and `/insights`.
- **Viewports & themes:** `1440×900` and `390×844` in both `light` and `dark` themes (8 cells total).
- **Results across all 8 cells:**
  - Horizontal overflow: **0** (`scrollWidth === innerWidth` at 1440 and 390 px).
  - Console / runtime errors: **0**.
  - `axe-core` WCAG 2.0/2.1/2.2 A & AA violations: **0 serious/critical, 0 total**.
  - Observable UI: Laporan (`SpendingTable`) rendered `GANTI UANG BELUM DITAUTKAN −Rp 150.000` (CSS-uppercased from `Ganti uang belum ditautkan`) reconciling `Rp 720.000 − Rp 150.000 = Rp 570.000` (`TOTAL PENGELUARAN`); Analisis (`KakeiboRows`) rendered `Ganti uang belum ditautkan` `−Rp 125.000`.

## Not run / caveats

- No production writes, archives, pushes, or pull requests were executed (awaiting owner acceptance).
