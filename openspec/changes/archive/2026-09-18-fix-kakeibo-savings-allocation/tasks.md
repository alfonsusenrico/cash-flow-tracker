# Tasks

## 1. Backend Logic Updates

- [x] 1.1 Update `backend/app/routers/movements.py` to auto-create `Internal Movement` categories with `kakeibo_type = None` instead of `'saving'`, and verify unit tests.
- [x] 1.2 Harden `saving_transfers` query in `backend/app/routers/dashboard.py` with `COALESCE(t.notes, '')` and ensure liquid internal movements do not count toward savings.

## 2. Database Migration Script

- [x] 2.1 Create a versioned SQL migration script in `backend/migrations/` to update `categories.kakeibo_type = NULL` for `Internal Movement` and reset `kakeibo_type = NULL` on existing liquid movements and pre-auth holds.
- [x] 2.2 Verify migration script execution on database.

## 3. Testing & Verification

- [x] 3.1 Write a dedicated unit test in `backend/tests/test_kakeibo_savings.py` verifying that liquid transfers and pre-auth transactions are excluded from Kakeibo savings calculations.
- [x] 3.2 Run test suite (`pytest`) and ensure all tests pass.
- [x] 3.3 Validate OpenSpec change with `openspec validate fix-kakeibo-savings-allocation`.
