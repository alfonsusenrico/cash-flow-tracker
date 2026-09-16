# notification-filtering Specification

## Purpose
TBD - created by archiving change strict-financial-notification-filtering. Update Purpose after archive.
## Requirements
### Requirement: Discard Non-Financial Notifications at Listener Entrance
The companion mobile notification listener SHALL discard any status bar notification that is not a settled financial transaction before database persistence or network synchronization.

#### Scenario: Promotional or marketing notifications are dropped immediately
- **GIVEN** a notification from a registered package (e.g. `com.gojek.app` or `com.shopeepay.id`)
- **WHEN** the notification contains promotional keywords, marketing slogans, ride discounts, or lacks a confirmed transaction amount (e.g. "GoRide harga pelajar", "Kabar baik buat Alfonsus!", "Fix irit ongkos")
- **THEN** the listener SHALL log the rejection at debug level
- **AND** the listener SHALL NOT insert the notification into the local Room database
- **AND** the listener SHALL NOT schedule or enqueue any background sync job.

#### Scenario: Verified transaction notifications are captured and synced
- **GIVEN** a notification from a registered package (e.g. `com.gojek.app` or `com.bca`)
- **WHEN** the notification matches a verified transaction pattern with a positive numeric amount (e.g. "Kamu berhasil transfer ke TABUNGAN BY JAGO", "Rp500.000 udah dikirim ke BCA", "Pengeluaran sebesar IDR 500,000.00")
- **THEN** the listener SHALL extract the structured fields (`event_class`, `expected_amount`, `expected_direction`, `expected_counterparty`)
- **AND** the listener SHALL persist the entity to the local database with `is_financial = true`
- **AND** the listener SHALL schedule immediate synchronization to the backend.

### Requirement: Backend Rejection of Non-Financial Payloads
The backend ingestion endpoint (`POST /api/notifications`) SHALL NOT persist any ingested payload into the `notification_events` table unless it represents a verified financial transaction with a valid monetary amount.

#### Scenario: Non-financial or noise event payloads are acknowledged without database storage
- **GIVEN** an ingestion request payload received by `POST /api/notifications`
- **WHEN** `parse_notification()` resolves `is_financial = false`, `event_class = 'noise'`, or `amount` is null / zero
- **THEN** the API SHALL skip inserting the event into the `notification_events` table
- **AND** the API SHALL NOT create any ledger transaction
- **AND** the response SHALL return success with `inserted = 0`.

