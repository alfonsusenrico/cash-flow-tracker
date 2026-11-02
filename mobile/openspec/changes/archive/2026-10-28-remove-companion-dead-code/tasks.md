# Tasks

## 1. Dead code and names

- [x] 1.1 Count every declaration in the network service, models, DAOs, inbox view model and preferences across all source sets, and remove the items D1 lists.
  - Evidence: the usage scan in `verification.md`; `compileDebugKotlin`, `compileDebugUnitTestKotlin` and `compileDebugAndroidTestKotlin` succeed after the removal.
- [x] 1.2 Point `SenderReplyPersistenceTest` at the production retention query instead of the removed `purgeOldSynced`.
  - Evidence: `assembleDebugAndroidTest` compiles it.
- [x] 1.3 Keep the `server_transaction_id` field with a comment naming its replacements (D2).
  - Evidence: `git log` shows the last writer removed in `7bf48c9`; `schemas/7.json` is unchanged and Room's schema check passes in the migration tests.
- [x] 1.4 Rename the inbox screen and view model (D3) and remove stale imports.
  - Evidence: no `Debug` identifier is left in `src/main` code; the import scan reports nothing; `DebugNotificationReceiver` is still only in `src/debug`.

Expected Result: only code something uses remains, under domain names, and every source set compiles.

## 2. Transport security

- [x] 2.1 Add the release network security configuration that denies cleartext, the debug override that allows it, and reference it from the manifest (D4).
  - Evidence: the merged release and debug manifests and the packaged `res/xml/network_security_config.xml` of each APK, in `verification.md`.

Expected Result: release builds refuse plain HTTP, and debug builds still reach a local backend.

## 3. Inbox labels

- [x] 3.1 Add `OwnerLabels` and `NotificationDao.saveOwnerLabel`, and route the send and edit paths through them (D6).
  - Evidence: `OwnerLabelsTest` (6 tests). With the old rule restored (counterparty cleared, direction `out`), 4 of them fail.
- [x] 3.2 Hide the account and category pickers for an unsent row, explain why, and drop the unused parameters (D5).
  - Evidence: `InboxSendFormUiTest` (2 tests). With the pickers shown again, the unsent-row test fails. `NotificationStatusUiTest` compiles with the new callback.

Expected Result: sending or editing from the sheet stores exactly what the owner chose and keeps every other captured label.

## 4. Auto-sync

- [x] 4.1 Add `AutoSync`, the `SyncScheduler` cancel functions, and call them from settings, app start, capture and listener reconnect (D7).
  - Evidence: `AutoSyncTest` (7 tests, WorkManager test driver). With cancel and the switch check removed, 2 of them fail.

Expected Result: with auto-sync off, nothing is sent unless the owner asks.

## 5. OpenSpec housekeeping

- [x] 5.1 Archive `complete-mobile-notification-results`, `add-sender-notification-replies` and `add-jago-source-pocket-confirmation`, in that order, after recording the implementing commits in their tasks.
  - Evidence: each `openspec archive <name> --yes` exited 0; commit `a81f46d`.
- [x] 5.2 Split the over-long requirements of `mobile-notification-completion`, `mobile-sender-replies` and `mobile-purpose-questions` through this change's deltas (D8).
  - Evidence: `openspec validate remove-companion-dead-code --strict` exits 0.

Expected Result: no change is left open from earlier work, and the specs can pass strict validation once this change is archived.

## 6. Release and verification

- [x] 6.1 Set version 1.8.1 (code 16) and update the version assertion (D9).
  - Evidence: `PurposeAnswersTest` passes with `1.8.1`; dexdump of the debug APK shows `const-string "1.8.1"` in `BuildConfig`, `ApiClient` and `NotificationProcessor`.
- [x] 6.2 Run the full unit suite, compile the instrumented tests, build the debug and release APKs without the build cache, and check the merged manifests.
  - Evidence: `verification.md`.

Expected Result: a verified 1.8.1 build and evidence for every behaviour change.

## Workflow follow-up

- Archive this change, then run `openspec validate --all --strict`.
- Commit, push `chore/companion-cleanup`, and report the compare link. The owner merges and installs; no device installation is part of this change.
