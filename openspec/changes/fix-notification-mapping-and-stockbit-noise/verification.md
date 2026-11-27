# Verification: fix-notification-mapping-and-stockbit-noise

- **Branch:** `fix/notification-mapping-and-stockbit-noise` (base `c138e1e7`, working tree ready for commit)
- **Date:** 2026-11-27

## Automated checks

- **OpenSpec strict validation:** `openspec validate --all --strict` — **87 passed, 0 failed**.
- **Backend full test suite (PostgreSQL 16, Flyway v59):**
  `TEST_DATABASE_URL="postgresql://cft:***@127.0.0.1:54399/cft_test" SKIP_TEST_MIGRATIONS=1 REQUIRE_TEST_DATABASE=1 PYTHONPATH=. pytest -q`
  — **2474 passed, 0 failed, 0 skipped** (315.25s), including all 5 new regression tests in `tests/test_notification_mapping_and_stockbit.py` and 243/243 notification suite tests.

## Not run / caveats

- Frontend checks and browser checks are not applicable (backend-only change; no files under `frontend/` were modified).
- No production writes, archives, pushes, or pull requests were executed (awaiting owner acceptance).
