## Why

Currently, notification parsing is duplicated between Kotlin regex in the mobile companion app and Python regex in the backend server. This duality causes subtle divergences (such as Unicode quotation marks vs ASCII single quotes, or missing pocket keywords), creates double-maintenance overhead, and leads to miscategorized transactions (e.g. Bank Jago internal pocket transfers being misclassified as "Makanan & Minuman" expenses). 

By establishing an API-First Headless Architecture, the backend completely removes regex parsing and serves strictly as a public-standard REST API that accepts structured transaction payloads with flexible entity resolution (supporting either IDs or human-friendly names) and idempotency guarantees. The mobile app becomes the single source of truth for on-device notification filtering and regex extraction, directly posting structured transaction requests.

## What Changes

- **Backend Public API (`POST /api/transactions`)**:
  - Accept either UUID or human-readable names: `account_name`, `target_account_name`, and `category_name`.
  - Accept `idempotency_key` (max 64 chars) to guarantee idempotency and prevent duplicate transaction creation on repeated payloads/replays.
  - Automatically resolve account and pocket names, supporting canonical pocket matching, synonym mapping, and auto-provisioning child pockets under parent institutions.
  - Automatically resolve category names, ensuring `"Internal Movement"` and `"Investasi"` exist in the database (auto-seeding if missing).
  - Enforce professional HTTP validation and error status codes (400, 422) for invalid payloads.
- **Backend Database Schema**:
  - Add `idempotency_key` column to `transactions` table with unique index `(user_id, idempotency_key) WHERE idempotency_key IS NOT NULL`.
  - Add `"Internal Movement"` and `"Investasi"` to `DEFAULT_CATEGORIES` in `backend/app/db/init_db.py`.
- **Backend Decommissioning**:
  - Decouple transaction creation completely from `notification_parser.py` (zero backend regex parsing).
- **Mobile Companion App (`financial-tracker-mobile-listener`)**:
  - Update `NotificationProcessor.kt` to act as the smart API client, extracting transaction intents on-device and building standard `POST /api/transactions` requests.
  - Implement full scenario mapping for Bank Jago (single/dual pocket transfers, inbound), GoPay, BCA, ShopeePay, and Stockbit.
  - Update Retrofit API service and sync worker to execute `POST /api/transactions`.
  - Store local transaction summary and sync status (`PENDING`, `SYNCED`, `FAILED`) without retaining redundant raw request payloads.

## Capabilities

### New Capabilities
- `idempotent-public-transactions`: Enables external clients (companion apps, bots, automations) to create transactions idempotently via `POST /api/transactions` using either UUIDs or human-readable entity names with automatic pocket and category resolution.

### Modified Capabilities
- `transaction-ledger-management`: Enhanced transaction creation contract to support idempotency keys and name-based entity resolution alongside existing UUID parameters.

## Impact

- **Backend Code**: `backend/app/routers/transactions.py`, `backend/app/db/init_db.py`, `backend/tests/test_transactions.py`.
- **Mobile Code**: `financial-tracker-mobile-listener` (`NotificationProcessor.kt`, `NotificationSyncWorker.kt`, `IngestionApiService.kt`, `ApiModels.kt`, tests).
- **Database**: New column and unique index on `transactions(idempotency_key)`.
- **Security & Privacy**: Zero regex logic or raw notification parsing on server; Bearer API key authentication remains standard.
