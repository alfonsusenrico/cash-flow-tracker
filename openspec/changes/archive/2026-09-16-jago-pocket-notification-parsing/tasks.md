## 1. Mobile Companion App Updates

- [x] 1.1 Update `NotificationProcessor.kt` in `financial-tracker-mobile-listener` to support dual-pocket, single-pocket OUT, and single-pocket INTO regexes
- [x] 1.2 Compile debug APK with `./gradlew assembleDebug`
- [x] 1.3 Install updated APK on connected device via `adb install -r`

## 2. Backend Parser and Ingest Route Updates

- [x] 2.1 Update `notification_parser.py` with `JAGO_DUAL_POCKET_PATTERN`, `JAGO_OUT_POCKET_PATTERN`, and `JAGO_IN_POCKET_PATTERN`
- [x] 2.2 Update `routers/ingest.py` pocket matching and transfer destination handling for single-pocket moves
- [x] 2.3 Add unit tests in `backend/tests/test_notification_parser.py` for single-pocket OUT and single-pocket INTO notifications
- [x] 2.4 Run pytest suite to verify all tests pass

## 3. End-to-End Verification on Device and Server

- [x] 3.1 Trigger notification rescan on device to process the active Bank Jago notification
- [x] 3.2 Verify notification capture and sync in mobile app and backend
- [x] 3.3 Validate and archive OpenSpec change
