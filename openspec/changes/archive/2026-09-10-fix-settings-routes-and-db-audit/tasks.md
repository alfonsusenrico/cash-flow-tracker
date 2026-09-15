## 1. Backend Route Mounting & Payday Flexibility

- [x] 1.1 Mount `auth_router` in `backend/app/main.py` under both `prefix` and `f"{prefix}/auth"` to support `/settings`, `/me`, `/api-key`, etc. directly and under `/auth/*`
- [x] 1.2 Expand `payday_day` validation in `SettingsUpdate` and `PaydayUpdate` in `backend/app/routers/auth.py` to allow `ge=1, le=31`
- [x] 1.3 Update `get_cycle_window` in `backend/app/routers/pulse.py` to dynamically clamp `payday_day` using `calendar.monthrange(year, month)[1]` for 1–31 payroll support

## 2. HTTP Method Parity on Mutation Endpoints

- [x] 2.1 Add `@router.patch` decorator alongside `@router.put` on `update_account` in `backend/app/routers/accounts.py`
- [x] 2.2 Add `@router.put` decorator alongside `@router.patch` on `update_transaction` in `backend/app/routers/transactions.py`
- [x] 2.3 Add `@router.put` decorator alongside `@router.patch` on `update_goal` in `backend/app/routers/goals.py` and `update_obligation` in `backend/app/routers/obligations.py`

## 3. Frontend Settings Modal & Integration Alignment

- [x] 3.1 Update `SettingsModal.tsx` to use `/auth/settings`, `/auth/me`, `/auth/api-key`, `/auth/api-key/reset`
- [x] 3.2 Update `SettingsModal.tsx` input bounds to `min={1} max={31}` and label to `Tanggal Gajian (Tgl 1–31)`
- [x] 3.3 Update `login/page.tsx` auth check to call `/auth/me`

## 4. Automated Testing & Verification

- [x] 4.1 Run backend pytest suite with new tests covering payday days 29, 30, 31 and dual PUT/PATCH endpoints
- [x] 4.2 Run frontend type-check, lint, and build
- [x] 4.3 Rebuild docker containers and smoke test changing payroll date to 29 in the live application
