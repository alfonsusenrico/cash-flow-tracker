## Why

Users attempting to change their payroll date (e.g. to the 29th) receive a `404 Not Found` error. An audit of frontend and backend integration points revealed three core issues:
1. Route path mismatch: `SettingsModal.tsx` calls `/settings`, `/me`, `/api-key`, and `/api-key/reset`, but the backend only mounts `auth_router` with a `/auth` prefix (e.g. `/api/auth/settings`).
2. Payday range limitation: `payday_day` was constrained to 1–28 in both Pydantic models and frontend inputs, rejecting valid payroll dates (such as 29, 30, 31, or end of month) common in Indonesia and globally.
3. HTTP method asymmetry: frontend components send `PATCH` to `/accounts/{id}` and `PUT` to `/transactions/{id}`, but the backend only registered `PUT` on accounts and `PATCH` on transactions, risking `405 Method Not Allowed`.

## What Changes

- **Add Direct Route Aliases & Path Normalization**: Support `/settings`, `/me`, `/api-key`, `/api-key/info`, and `/api-key/reset` directly under `/api` (in addition to `/api/auth/*`) in the backend, and align frontend `SettingsModal.tsx` and `login/page.tsx` calls.
- **Support Flexible Payday Range (1–31)**:
  - Update `SettingsUpdate` and `PaydayUpdate` validations to accept `payday_day` between 1 and 31.
  - Enhance `get_cycle_window` in `pulse.py` to dynamically clamp `payday_day` to `calendar.monthrange(year, month)[1]` for each month (handling 28/29 for Feb, 30 for Apr/Jun/Sep/Nov, 31 for others).
  - Update `SettingsModal.tsx` UI label to `Tanggal Gajian (Tgl 1–31)` and set `max={31}`.
- **Ensure HTTP Method Parity Across Resources**:
  - Add `@router.patch` to `update_account` in `accounts.py` alongside `@router.put`.
  - Add `@router.put` to `update_transaction` in `transactions.py` alongside `@router.patch`.
  - Add `@router.put` to `update_goal` in `goals.py` and `update_obligation` in `obligations.py`.
- **Comprehensive Database Integrity Verification**:
  - Audit all database SQL statements for accounts, categories, transactions, goals, obligations, dashboard, and pulse to ensure transaction balance calculations, constraint checking, and foreign key cascades are robust and complete.

## Capabilities

### Modified Capabilities
- `clean-core-ledger`: Expand payday cycle window to support days 1–31 with month-aware clamping, expose direct user settings and API key endpoints, and support dual `PUT`/`PATCH` mutation methods on accounts and transactions.

## Impact

- **Backend**:
  - `backend/app/main.py`: Include `auth_router` under direct API prefix for root auth/settings endpoints.
  - `backend/app/routers/auth.py`: Update validation bounds to `1 <= payday_day <= 31`.
  - `backend/app/routers/pulse.py`: Month-aware clamp in `get_cycle_window`.
  - `backend/app/routers/accounts.py`: Support `PATCH` alongside `PUT`.
  - `backend/app/routers/transactions.py`: Support `PUT` alongside `PATCH`.
  - `backend/app/routers/goals.py` & `obligations.py`: Support `PUT` alongside `PATCH`.
- **Frontend**:
  - `frontend/src/components/ui/SettingsModal.tsx`: Update API endpoints and payday day input max to 31.
  - `frontend/src/app/auth/login/page.tsx`: Explicitly call `/auth/me`.
- **Tests**:
  - `backend/tests/test_personal_finance_os.py` and `backend/tests/test_clean_core.py`: Add test cases for payday days 29, 30, 31 and HTTP method parity.
