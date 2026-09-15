## Context

When changing settings (such as the payroll date), the frontend currently receives a `404 Not Found`. Additionally, entering a day such as 29 triggers input validation failures. A thorough audit of the backend routes, database processes, and frontend callers identified four areas needing structural hardening:
1. Backend route mounting mismatch (`/api/settings` vs `/api/auth/settings`).
2. Payday date clamping limited to 28 rather than the full calendar month (1–31).
3. HTTP method mismatches between frontend mutation verbs (`PATCH` vs `PUT`).
4. Database process integrity across accounts, categories, transactions, goals, and obligations.

## Goals / Non-Goals

**Goals:**
- Provide seamless route resolution for user settings, profile, and API key management under both `/api/*` and `/api/auth/*`.
- Support payroll dates 1–31 with month-aware clamping in cycle calculations (`get_cycle_window`).
- Establish HTTP verb symmetry across all update endpoints (`accounts`, `transactions`, `goals`, `obligations`) supporting both `PUT` and `PATCH`.
- Verify and harden all database transactions, cascade behaviors, and balance recalculations.

**Non-Goals:**
- We are NOT modifying the minimal 5-table + 2-extension schema layout.
- We are NOT introducing complex cron auto-transfer engines or background recurring job workers.

## Decisions

### 1. Direct API Mounts & Frontend Alignment
- **Decision**: In `backend/app/main.py`, mount `auth_router` with both direct `prefix=prefix` (providing `/api/settings`, `/api/me`, `/api/api-key`, etc.) and `prefix=f"{prefix}/auth"`. Simultaneously update `SettingsModal.tsx` and `login/page.tsx` to use `/auth/*` explicitly.
- **Rationale**: This dual strategy guarantees that both existing clients (or cached frontend assets) calling `/api/settings` and new frontend builds calling `/api/auth/settings` succeed without 404s.

### 2. Payday Calendar Range 1–31 with Dynamic Month Clamping
- **Decision**: Allow `payday_day: int = Field(ge=1, le=31)` in `SettingsUpdate` and `PaydayUpdate`. In `get_cycle_window`, clamp `payday_day` to `calendar.monthrange(year, month)[1]`.
- **Rationale**: In real-world personal finance, payday dates fall on the 25th, 28th, 29th, 30th, or 31st (end of month). Clamping dynamically at runtime ensures that February safely uses day 28 (or 29 in leap years) while 30- and 31-day months respect the user's exact payday.

### 3. Dual PUT and PATCH Decorators on All Updatable Entities
- **Decision**: Decorate account updates, transaction updates, goal updates, and obligation updates with both `@router.put` and `@router.patch`.
- **Rationale**: Prevents frontend/client HTTP 405 errors regardless of which mutation verb is dispatched by React Query mutation hooks.

### 4. Database Process Integrity Verification
- **Decision**: Ensure that all database updates:
  1. Calculate account balances atomically via `initial_balance + income - expense - transfer_out + transfer_in`.
  2. Transaction updates cleanly reverse prior goal and obligation effects before applying new ones.
  3. Deletions check for referenced transactions before deciding between soft-archive or hard-delete.

## Risks / Trade-offs

- **[Risk]** Leap years or short months causing cycle date edge cases.
  - **Mitigation**: Standardize on `calendar.monthrange(year, month)[1]` to compute the exact last day of the given calendar month.
- **[Risk]** Route collision when mounting `auth_router` directly under `/api`.
  - **Mitigation**: `auth_router` endpoints (`/login`, `/register`, `/logout`, `/me`, `/settings`, `/payday`, `/api-key`, `/api-key/reset`) have distinct path names that do not overlap with any other domain routers (`/accounts`, `/categories`, `/transactions`, `/goals`, `/obligations`, `/dashboard`, `/pulse`).
