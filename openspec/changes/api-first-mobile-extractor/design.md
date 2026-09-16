## Context

Notification-based cash flow tracking originally implemented regex matching and entity parsing on both the Android companion app (`financial-tracker-mobile-listener`) and the Python backend (`cash-flow-tracker`). This led to:
1. Redundant parsing overhead and dual-maintenance of regex patterns in Kotlin and Python.
2. Divergent behavior due to platform differences (e.g. Unicode curly quotes `’` vs ASCII single quotes `\x27`).
3. Miscategorization bugs, such as internal Bank Jago transfers resolving to "Makanan & Minuman" expenses due to missing seed categories and fallback loops.

We are shifting to an **API-First Headless Architecture**. The backend server is purely a standard public REST API, completely devoid of regex pattern matching or notification-specific parsing. The mobile companion is a smart client that handles on-device interception, regex extraction, and issues standard `POST /api/transactions` calls.

## Goals / Non-Goals

**Goals:**
- Eliminate all regex operations and notification parsing from the backend server.
- Enhance `POST /api/transactions` to accept either UUIDs or human-friendly entity names (`account_name`, `target_account_name`, `category_name`).
- Provide first-class idempotency via an optional `idempotency_key` (UUID or hash string) on `POST /api/transactions`.
- Ensure standard, predictable error handling (HTTP 400, 422, 404) for all invalid or unresolvable payloads.
- Update the mobile companion app to extract transaction data on-device and directly submit standard `POST /api/transactions` calls.
- Auto-seed neutral categories (`"Internal Movement"`, `"Investasi"`) so transfers and investments are never misclassified as expenses.

**Non-Goals:**
- Removing the `notification_events` table completely (it remains available as an optional audit lake if desired, but is decoupled from core ledger creation).
- Modifying UI layouts, color palettes, or frontend components.
- Auto-scheduling or background recurring transaction mechanics (out of scope).

## Decisions

### 1. Unified `POST /api/transactions` with Dual Identification (UUID or Name)
**Decision**: Allow clients to provide either UUID or human-readable names for accounts, target accounts, and categories.
- `account_id` OR `account_name`
- `transfer_target_account_id` OR `target_account_name`
- `category_id` OR `category_name`
- `idempotency_key` (optional, max 64 chars)

*Rationale*: Companion apps, bot automations, and third-party integrations should not be forced to query and maintain internal Postgres UUID mappings for user-facing entities like "Bank Jago", "Dana Darurat", or "Internal Movement". Resolving names server-side with canonical matching and synonym support provides a clean, professional public API developer experience.

*Alternatives Considered*:
- *Require mobile app to query `/api/accounts` and `/api/categories` to obtain UUIDs first*: Adds latency, network roundtrips, local caching complexity on mobile, and fails if a new pocket needs to be created on the fly.
- *Create a separate endpoint like `/api/transactions/quick`*: Unnecessary endpoint proliferation. Enhancing `POST /api/transactions` keeps the API unified as mandated by `AGENTS.md`.

### 2. Strict Server-Side Idempotency
**Decision**: Add an `idempotency_key` column with a partial unique index on `transactions(user_id, idempotency_key) WHERE idempotency_key IS NOT NULL`.
- If an incoming `POST /api/transactions` has an `idempotency_key` that already exists for the user, the server returns the existing transaction immediately with HTTP 200 OK.
- Mobile companion supplies the SHA-256 payload hash as the `idempotency_key`.

*Rationale*: Prevents duplicate ledger entries during network retries, Android broadcast replays, or background worker retries.

### 3. Smart Client Responsibilities (Mobile Extractor)
**Decision**: The Android companion app (`NotificationProcessor.kt`) becomes the single source of truth for all notification regexes. It parses the notification text, determines `type` (`expense`, `income`, `transfer`), extracts amounts, resolves counterparties, and directly builds the `POST /api/transactions` request.

*Rationale*: Eliminates duplicate code and platform divergence. Banking format changes only require updating the Kotlin processor.

## Risks / Trade-offs

- **[Risk] Unresolvable account name provided by client** → **Mitigation**: Server validates `account_name`. If it cannot match an existing account, pocket synonym, or auto-provision a child pocket under a matched institution, it returns HTTP 400 Bad Request with a clear message: `{"detail": "Could not resolve account '<name>'"}`.
- **[Risk] Missing category for internal movement** → **Mitigation**: Server automatically seeds `"Internal Movement"` and `"Investasi"` categories if missing for the user when requested, ensuring transfers never fall back to living spending categories like "Makanan & Minuman".
- **[Risk] Replay of distinct transactions with same key** → **Mitigation**: Mobile computes `payload_hash` from `packageName|title|bodyText|postTime`. Distinct transactions have distinct timestamps or text and produce distinct hashes.

## Migration Plan

1. Database schema migration: Add `idempotency_key` column and unique index to `transactions`.
2. Update backend `init_db.py` to include `"Internal Movement"` and `"Investasi"` in `DEFAULT_CATEGORIES`.
3. Update backend `transactions.py` with name resolution, idempotency checking, and error responses.
4. Update mobile companion app to call `POST /api/transactions`.
5. Run automated tests across both repositories.
