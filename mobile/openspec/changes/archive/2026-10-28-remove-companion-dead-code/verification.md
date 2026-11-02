# Verification: remove-companion-dead-code

Date: 2026-10-28

## Revision

- **Base:** `afcc0db` (`origin/main`, 1.8.0, code 15, Room 7).
- **Branch:** `chore/companion-cleanup`. The old-change archives are commit `a81f46d`. The results
  below come from the uncommitted working tree that the next commit records.
- **Environment:** JDK 17 (`/opt/homebrew/opt/openjdk@17`), `ANDROID_HOME` set to the local SDK
  (no `local.properties`), build-tools 37.0.0 for `aapt2` and `dexdump`, OpenSpec CLI 1.14.1,
  Robolectric 4.14.1 with SDK 34.
- No device was used, and nothing was installed.

## Bugs confirmed on the base

- `DebugInboxViewModel.saveAndSyncUnsynced` took `accountId` and `categoryId` and never used them.
  It and `updateSyncedTransaction` called `updateLabel` with `expectedCounterparty = null` and a
  direction of `in` or `out`.
- `SettingsScreen` only wrote `isAutoSyncEnabled` and scheduled work when the switch was turned on.
  `FinancialNotificationListenerService.onListenerConnected` started `DirectSync` and an immediate
  sync without checking the switch.

## Usage evidence for removals

A word-boundary count across `app/src` on the base and on the result. "Base" counts every
occurrence; 1 means the declaration alone.

| Name | Base | Now |
| --- | --- | --- |
| `createTransaction`, `createMovement` | 1 each | 0 |
| `TransactionCreateRequest`, `TransactionResponse`, `MovementRequest`, `MovementResponse` | 3 each (declaration, import, dead method) | 0 |
| `getNotificationsByPackageFlow`, `deleteEmptyNotifications`, `getByPayloadHash`, `nextCompletionAt`, `markAsSynced`, `updateServerTransactionId`, `updateSyncError`, `deleteDebugRecords` | 1 each | 0 |
| `purgeOldSynced` | 2 (declaration, one instrumented test) | 0 |
| `updateLabel` | 4 (declaration, `saveLabel`, the two defective paths) | 0 |
| `saveLabel` | 1 | 0 |
| `DebugInboxScreen`, `DebugInboxViewModel` | 6, 7 | 0 |
| `usesCleartextTraffic` | 1 | 0 |

An import scan (every `import` whose simple name is not used in the file) reported eight stale
imports, all removed, and reports none now.

## Unit tests

Command: `./gradlew clean testDebugUnitTest --no-build-cache --rerun-tasks`, exit status 0.

- **Totals across the result XML:** 153 tests, 0 skipped, 0 failures, 0 errors.
- **Timestamps:** `2026-10-28T09:32:41` to `09:32:45` (UTC).
- **Baseline on `afcc0db`:** 138 tests, all passing (`09:18:39` to `09:18:43`).

| New suite | Tests | Covers |
| --- | --- | --- |
| `OwnerLabelsTest` (Robolectric, in-memory Room) | 6 | An unsent pocket move keeps its counterparty and `internal` direction. A type change to income sets `in` and keeps the counterparty. A kept row joins the send pipeline as `owner_sent`. A sent row is not relabelled. An accepted ledger edit keeps the counterparty. The direction rule. |
| `AutoSyncTest` (Robolectric, WorkManager test driver) | 7 | Off cancels the periodic sync and a queued send. Capture and reconnect start nothing while off. They start a send while on. Nothing is sent without a connected account. App start cancels leftover periodic work when off and schedules it when on. A manual sync still queues while off. |
| `InboxSendFormUiTest` (Compose on Robolectric) | 2 | An unsent row shows no "Rekening" or "Kategori" picker, shows the explanation, and sends exactly amount, type and note. A reviewable row still offers both pickers. |

`PurposeAnswersTest` now asserts the `X-Companion-Version` header `1.8.1`.

### Regression proof

The old behaviour was restored temporarily in the working tree (counterparty cleared and direction
`out` in the label save, no cancel and no switch check in `AutoSync`, pickers always shown). The
three new suites then had 7 failures out of 15:

- `OwnerLabelsTest`: 4 failures;
- `AutoSyncTest`: 2 failures;
- `InboxSendFormUiTest`: 1 failure.

The files were restored afterwards. All 153 tests pass on the final tree.

## Builds

Each run used `--no-build-cache`, after the `clean` above:

| Command | Exit |
| --- | --- |
| `./gradlew assembleDebug --no-parallel` | 0 |
| `./gradlew assembleRelease --no-parallel` | 0 |
| `./gradlew assembleDebugAndroidTest --no-parallel` | 0, which compiles every instrumented test, including the updated `NotificationStatusUiTest`, `InboxSwipeDeleteTest` and `SenderReplyPersistenceTest` |
| `./gradlew lintDebug --no-parallel` | 0, with 45 warnings and no errors |

- The lint warnings are newer-dependency notices and pre-existing items, plus
  `InsecureBaseConfiguration` on the debug-only network configuration, which is intended.
- One earlier combined run of every task at once failed with `OutOfMemoryError` in D8 dex merging
  under the 2 GB Gradle heap. That is an environment limit, so the tasks were rerun one by one.
  `gradle.properties` is unchanged.

## APK and manifest checks

| Check | Debug (`app-debug.apk`) | Release (`app-release-unsigned.apk`) |
| --- | --- | --- |
| `aapt2 dump badging` | `com.alfonsusenrico.financialtracker.listener.debug`, code 16, 1.8.1 | `com.alfonsusenrico.financialtracker.listener`, code 16, 1.8.1 |
| Merged manifest (`merged_manifests/<variant>`) | `networkSecurityConfig="@xml/network_security_config"`, no `usesCleartextTraffic`, `DebugNotificationReceiver` declared | `networkSecurityConfig="@xml/network_security_config"`, no `usesCleartextTraffic`, no debug receiver |
| Packaged network security configuration | `base-config cleartextTrafficPermitted=true` | `base-config cleartextTrafficPermitted=false` (resource `res/8G.xml`) |
| `dexdump -d` | `const-string "1.8.1"` in `ApiClient` and `NotificationProcessor`; `BuildConfig.VERSION_NAME` field present | same |

## OpenSpec

- `openspec archive` of `complete-mobile-notification-results`, `add-sender-notification-replies`
  and `add-jago-source-pocket-confirmation`: each exited 0 and created its main spec, with the
  Purpose text from the change.
- `openspec validate remove-companion-dead-code --strict`: exit 0.
- Before this change is archived, `openspec validate --all --strict` exits 1, because the main
  specs still hold the long requirements that this change splits.

## Archive

- `openspec archive remove-companion-dead-code --yes`: exit 0. It added 29 requirements, modified
  10, and created `companion-transport-security`.
- `openspec validate --all --strict` after the archive: exit 0, with all 6 specs passing and no
  open change.

## Not verified

- No instrumented test was run, because no device was attached and installation was out of scope.
  They compile.
- The release build's refusal of `http://` was checked through the packaged configuration, not
  through a live request on a device.
