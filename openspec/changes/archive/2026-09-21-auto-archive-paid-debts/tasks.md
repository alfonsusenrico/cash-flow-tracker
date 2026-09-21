# Tasks

## 1. Backend Write-Path Amortization & Archival Sync

- [x] 1.1 In `backend/app/routers/transactions.py`: Update `create_transaction` to atomically set `is_archived = true` when payment reduces `remaining_amount` to 0 or less. Verify with focused pytest.
- [x] 1.2 In `backend/app/routers/transactions.py`: Update `update_transaction` and `delete_transaction` to atomically set `is_archived = false` if reversal restores `remaining_amount > 0`, and `is_archived = true` if new amount reduces it to 0. Verify with focused pytest.
- [x] 1.3 In `backend/app/routers/recurring.py`: Update recurring execution amortization to set `is_archived = true` when payment reduces `remaining_amount` to 0. Verify with focused pytest.
- [x] 1.4 In `backend/app/routers/obligations.py`: In `update_obligation`, auto-set `is_archived = true` if `remaining_amount == 0`, and `is_archived = false` if `remaining_amount > 0` (unless explicitly provided in payload). Verify with focused pytest.

## 2. Active Debt Query Filtering & Dashboard Consistency

- [x] 2.1 In `backend/app/routers/obligations.py`: In `list_obligations`, ensure `include_archived=false` filters `AND is_archived = false AND remaining_amount > 0`. Verify with focused pytest.
- [x] 2.2 In `backend/app/routers/dashboard.py`: In `monthly_commitments`, `obligations_glance`, and `net_worth`, filter `AND is_archived = false AND remaining_amount > 0`. Verify with focused pytest.

## 3. Database Migration & Historical State Cleanup

- [x] 3.1 In `backend/app/db/init_db.py`: Add startup migration statement `UPDATE obligations SET is_archived = true, updated_at = NOW() WHERE remaining_amount <= 0 AND is_archived = false;`. Verify startup execution.

## 4. End-to-End Verification & Contract Validation

- [x] 4.1 Write comprehensive regression tests in `backend/tests/test_personal_finance_os.py` covering payoff archival, reversal reactivation, and exclusion from active queries. Verify tests pass with `PYTHONPATH=. backend/.venv/bin/pytest backend/tests`.
- [x] 4.2 Run `openspec validate auto-archive-paid-debts` and verify specification integrity.
- [x] 4.3 Rebuild backend docker container (`docker compose build api && docker compose up -d --no-deps api`) and verify live behavior.
