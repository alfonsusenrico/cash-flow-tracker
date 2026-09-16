# Proposal: Strict Financial Notification Filtering & Noise Elimination

## Why
The Android companion listener currently captures and syncs all push notifications originating from whitelisted target package names (such as Gojek, Shopee, Stockbit, and Bank Jago), including non-financial promotional campaigns, marketing ads, loan offers, stock market news headlines, and ride discount vouchers (e.g. "GoRide harga pelajar", "Kabar baik buat Alfonsus!", "Fix irit ongkos"). 

Because `NotificationProcessor.kt` and `notification_parser.py` defaulted to `isFinancial = true` when no specific pattern matched, non-financial noise was stored in the device's Room database and synced to the production server's `notification_events` table. The user has explicitly instructed that the mobile app and listener must ignore unrelated notifications and must not store or keep them.

## What Changes
1. **Mobile Listener Ingestion Firewall (`FinancialNotificationListenerService.kt` & `NotificationProcessor.kt`)**:
   - Reject promotional, marketing, and non-transaction push notifications at the listener entrance.
   - Require notifications to match verified financial transaction criteria with a determinable monetary amount (> 0) and settled transaction event class (`expense`, `income`, `transfer`).
   - If a notification does not match a verified financial transaction, discard it immediately. Do NOT persist it to the SQLite Room database and do NOT enqueue sync.
2. **Mobile Database Cleanup**:
   - Provide an automatic or one-tap purge mechanism in the mobile app to delete existing non-financial / noise notifications from the local Room database (`raw_notifications`).
3. **Backend Notification Parser & Ingestion Gate (`notification_parser.py` & `routers/ingest.py`)**:
   - Update `parse_notification` fallback to return `is_financial=False`, `event_class="noise"`.
   - Update `ingest_notifications` endpoint in `routers/ingest.py` to only insert into `notification_events` when `parsed.is_financial == True` and `parsed.amount > 0` and `parsed.event_class != 'noise'`. Non-financial payloads will be acknowledged and discarded without persisting to the database.
4. **Production Server & Local Database Purge**:
   - Clean up existing noise records from `notification_events` where `transaction_id IS NULL` and `event_class = 'unknown'` or `is_financial = false`.

## Capabilities
- `notification-filtering`: Zero-noise notification capture ensuring only authentic, settled money transactions are processed, persisted, or synchronized.

## Impact
- **Mobile Listener App (`financial-tracker-mobile-listener`)**:
  - `FinancialNotificationListenerService.kt`: Expanded marketing and non-transaction rejection filters.
  - `NotificationProcessor.kt`: Strict gate preventing non-financial entities from being inserted into Room DB or triggering sync.
  - `NotificationDao.kt`: Query to purge non-financial / noise records.
  - `DebugInboxViewModel.kt` / `DebugInboxScreen.kt`: Clean feed displaying only actual financial movements.
- **Backend API (`cash-flow-tracker`)**:
  - `backend/app/services/notification_parser.py`: Noise fallback and strict validation.
  - `backend/app/routers/ingest.py`: Skip database insertion for non-transaction items.
- **Database**:
  - Existing noise rows cleaned up from `notification_events`. No schema migrations required.

## Expected Outcome
1. When promotional push notifications arrive on the user's phone (e.g., Gojek GoRide deals, ShopeePay cashback promos, Stockbit news flashes), the listener drops them silently in memory.
2. The local mobile app feed displays only genuine financial transactions (BCA transfers, Jago pocket movements, GoPay payments, Stockbit buy/sell matches).
3. The server database `notification_events` only stores verified financial transactions.
