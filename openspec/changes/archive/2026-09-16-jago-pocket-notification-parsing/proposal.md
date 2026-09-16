## Why

Bank Jago emits distinct notification formats when money is moved between pockets or in/out of pockets (such as `"You've moved Rp500.000 out of your My Emergency Fund Pocket."` or `"into your ... Pocket"`). Currently, the whitelist regex in both the mobile listener and the backend parser only matched dual-pocket phrasing (`Rp... has been moved from your ... to your ...`), causing single-pocket movements to be dropped as non-financial noise.

## What Changes

- Support all 3 Bank Jago pocket transfer notification formats (dual pocket move, single pocket "out of", and single pocket "into") in both the Android companion app (`NotificationProcessor.kt`) and backend parser (`notification_parser.py`).
- Add English <-> Indonesian synonym mapping and fallback matching in `routers/ingest.py` (e.g. mapping `"My Emergency Fund"` or `"Emergency Fund"` to child pocket `"Dana Darurat"`).
- Automatically link single-pocket outbound moves to the main account balance as destination, and single-pocket inbound moves from the main account balance as source.

## Capabilities

### Modified Capabilities
- `notification-filtering`: Expand whitelist transaction recognition to include Bank Jago single-pocket movement notifications ("out of" and "into" pockets) as confirmed financial transfers.

## Impact

- **Mobile App:** `NotificationProcessor.kt` in `financial-tracker-mobile-listener` will recognize Bank Jago pocket move notifications.
- **Backend Services:** `app/services/notification_parser.py` and `app/routers/ingest.py` will parse and map Jago single-pocket moves into internal ledger transfers between child pockets and the parent Jago account.
- **Testing:** Unit tests in `backend/tests/test_notification_parser.py` covering all Jago single and dual-pocket movement variations.
