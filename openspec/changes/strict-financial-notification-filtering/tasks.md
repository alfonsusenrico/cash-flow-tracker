# Tasks: Strict Financial Notification Filtering & Noise Elimination

## Phase 1: Mobile Listener Filtering & Gate Hardening (`financial-tracker-mobile-listener`)
- [x] 1.1 Expand early marketing & promo noise filter in `FinancialNotificationListenerService.kt` to drop ads, discounts, ride vouchers, and market news.
- [x] 1.2 Update `NotificationProcessor.kt` so `extractDetectionSummary()` defaults to `isFinancial = false`.
- [x] 1.3 Add strict discard gate in `processNotification()`: drop any notification where `!summary.isFinancial` or `expectedAmount == null || expectedAmount <= 0`.
- [x] 1.4 Add `deleteNonFinancialNoise()` in `NotificationDao.kt` and trigger on app launch to wipe historical promo notifications from Room DB.
- [x] 1.5 Verify mobile app builds cleanly with `./gradlew assembleDebug` and install update to connected device via `adb install -r`.
- **Expected Result Phase 1:** Promo notifications ("GoRide harga pelajar", "Fix irit ongkos", etc.) are deleted from device, new promos are dropped silently, and the feed shows only genuine transactions.

## Phase 2: Backend Parser & Ingestion Gate Hardening (`cash-flow-tracker`)
- [x] 2.1 Update `services/notification_parser.py` default fallback to return `is_financial=False, event_class="noise"`.
- [x] 2.2 Update `routers/ingest.py` to skip database insertion in `notification_events` if `not parsed.is_financial` or `final_amount is None`.
- [x] 2.3 Run backend tests with `pytest tests/` to ensure all existing notification parsing and transaction tests pass.
- **Expected Result Phase 2:** Backend API only persists verified financial transactions.

## Phase 3: Database Noise Purge & Production Deployment
- [x] 3.1 Clean up noise records (`event_class = 'unknown'` and `transaction_id IS NULL`) in local and production PostgreSQL (`notification_events`).
- [x] 3.2 Commit backend changes to `cash-flow-tracker` and push to `main` to trigger automated CI/CD deployment to `msi`.
- [x] 3.3 Commit mobile changes to `financial-tracker-mobile-listener`.
- [x] 3.4 Verify live server logs and device app feed to ensure complete noise elimination.
- **Expected Result Phase 3:** Both server and mobile listener store zero noise, production is cleanly deployed, and feed is 100% clean.
