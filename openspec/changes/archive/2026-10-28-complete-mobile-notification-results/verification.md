# Verification evidence

## Backend contract and release configuration

- Full backend regression on a disposable PostgreSQL 16 database migrated through Flyway V20: 413 passed, zero skipped.
- Final API image: `cash-flow-tracker-api@sha256:e320678e3bd12b627b583e34f6ef101e8b7706c9ef6e0facb86aa13b3cf4b1eb`.
- Authenticated fictional smoke covered mocked queued-to-recorded ordinary expense, stale lease fencing, redelivery, deterministic Jago bilateral movement, ledger visibility, and reconciliation flag. It did not use owner data or production.
- `transaction_id` is emitted only for an existing owned ordinary expense/income anchor. Paired movements, trades, and deleted transactions do not expose one.
- Operational AI remains disabled. The temporary mini diagnostic is recorded separately and did not meet the quality gate; no additional paid calls were made after benchmark deferral.

## Companion verification

- JDK 17: `testDebugUnitTest`, `lintDebug`, and `connectedDebugAndroidTest -PcompanionVerification=true` passed. The device run contained 14 tests for migrations, persistence, mock completion recovery, safe terminal handling, status UI, large-text Compose rendering, privacy boundaries, and guarded local API integration.
- The guarded local fixture used a disposable PostgreSQL database, a separate `.verification` app ID, only fictional events, and `NOTIFICATION_AI_ENABLED=false`. It verified authoritative recorded payloads, ordinary edit UUIDs, permission-denied alert recovery, and stable movement notification identity. Unit/instrumentation tests cover queued polling and retry responses without re-submission.
- The rebuilt release manifest has the listener and backup exclusions but no debug receiver or injection action. `git diff --check` and strict validation of both relevant OpenSpec changes pass.

## Device update

- Connected TECNO CN7c Android 16 updated with `adb install -r` from debug APK version 1.1.0 (version code 2).
- The private checksum-only comparison found Room upgraded from schema 2 to 3, all 15 original rows preserved, and stable pairing preferences preserved. Listener access and notification permission remained enabled.
- The owner explicitly declined TalkBack acceptance for this change. Large-text behavior is covered by the verification-app Compose test; no TalkBack device exercise or accessibility-conformance claim is made.
