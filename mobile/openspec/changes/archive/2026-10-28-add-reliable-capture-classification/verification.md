# Verification: add-reliable-capture-classification

Date: 2026-10-28

## Revision

- **Base:** `c2b8c33b66977128c7a792067c39cef6d4c1a896`, merged into `origin/main` as `1d69230`.
- **Branch:** `feat/reliable-capture-classification`. The results below were produced on the uncommitted working tree that the feature commit records.
- **Environment:**
  - `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`;
  - Android build-tools 37.0.0 for dexdump;
  - OpenSpec CLI `/opt/homebrew/bin/openspec`;
  - Robolectric 4.14.1 with SDK 34 (`android-all` downloaded on the first run).

## Unit tests (tasks 1.1–3.1)

Command: `./gradlew testDebugUnitTest --rerun-tasks`, exit status 0, `BUILD SUCCESSFUL`.

Totals across the result XML: 109 tests, 0 skipped, 0 failures, 0 errors. The timestamps are fresh, `2026-10-28T06:52:21` and `06:52:24` (UTC). The baseline on `c2b8c33` was 54 tests.

| Suite | Tests | Covers |
| --- | --- | --- |
| `CaptureClassifierTest` | 29 | Device-sample noise shapes. Transaction shapes. Safety footers. The five drop reasons. `parse_idr_amount` parity. Balance phrases. Inbox lines in big text. |
| `CaptureStoreTest` (Robolectric, in-memory Room) | 12 | Re-post update. Identical payments at different times. Legacy rescan by hash. Gojek/GoPay pairing, inside and outside 2 minutes. Case and spacing normalisation. One-to-one pairing, including a payment seen by only one app. Same-app repeats. Summary restatement. Feed, count and send-local behaviour for kept rows. 7-day prune. |
| `CaptureMigrationTest` (Robolectric) | 1 | A populated version 5 database (built from `5.json`) migrates to 6 and passes Room's open-time validation. Old rows read back as send rows with their hash unchanged. |
| `CaptureProcessorTest` (Robolectric) | 1 | The processor end to end: one send row, one kept row, two counted drops (one-time code, ongoing), and the failure counter. |
| `BatchIsolationTest` | 6 | A 422 with one bad row. A 503, a 401 and network loss during isolation. A single-row 422. A batch 500 that is not split. |
| `RetryingWriteTest` | 3 | Retry, then success. A single failure report after the last attempt. Cancellation is never swallowed. |
| `RegistryTest` | 5 (2 new) | Every lowercase package selectable once, with registry labels and descriptions. Institution siblings. |
| `PayloadHashTest` | 3 (1 new) | A golden value for SHA-256(`package|title|text|postTime`). Its expected value was computed independently with `shasum -a 256`. |
| Existing suites | 49 | Unchanged and passing: `NotificationProcessorTest` 28, `DirectSyncTest` 1, `MappingConfirmationTest` 3, `ProcessingReasonsTest` 5, `SenderQuestionsTest` 5, `SourcePocketQuestionsTest` 2, `NotificationResultContractTest` 5. |

### Mutation checks (the tests catch the faults they guard)

The production code was restored after each check, and the suites passed again.

1. **Migration index.** Removing the `index_raw_notifications_capture_outcome` creation from `MIGRATION_5_6` made `CaptureMigrationTest` fail with `IllegalStateException: Migration didn't properly handle: raw_notifications`.
2. **One-to-one pairing.** Removing the one-to-one claim clause from `findSiblingOriginal` made `CaptureStoreTest.aRowAbsorbsOnlyOneDuplicate` fail. Without the clause, a second payment seen only by the Gojek app would have been absorbed. The other 11 tests still passed, which shows the dedicated test is needed.

## Instrumented tests

`./gradlew assembleDebugAndroidTest`: exit status 0. The instrumented sources compile, including:

- `CompletionMigrationTest`, extended to version 6 with a new version 5 case;
- `NotificationStatusUiTest.keptNotificationCanBeSentToTracker`.

They were **not run**, because no device install is permitted for this phase.

## Build and version (task 5.1)

- `app/build.gradle.kts`: `versionCode = 14`, `versionName = "1.7.0"`.
- `./gradlew clean assembleDebug --no-build-cache`: exit status 0, `BUILD SUCCESSFUL`.
- APK: `app/build/outputs/apk/debug/app-debug.apk`, 18,416,955 bytes, SHA-256 `14628dca697f79c5a9ffcae0e0bbd125bdf66ffd0d64fb8bc9f77f211e968b71`.
- `dexdump` over every `classes*.dex`:

```text
classes11.dex  ApiClient.createService$lambda$0   const-string v3,  "1.7.0"
classes11.dex  NotificationProcessor.toEntity     const-string v26, "1.7.0"
classes5.dex   BuildConfig.VERSION_NAME = "1.7.0", VERSION_CODE = 14
```

No `"1.6.1"` string remains in any dex file.

## Schema

`app/schemas/.../AppDatabase/6.json` was exported by the build. It contains:

- `capture_outcome` with default `'send'`;
- `repost_count` with default `0`;
- indexes on `notification_key`, `normalized_fingerprint` and `capture_outcome`.

## OpenSpec (task 5.2)

| Command | Exit status | Result |
| --- | --- | --- |
| `openspec validate add-reliable-capture-classification --strict` | 0 | valid |
| `openspec validate --all` | 0 | 5 passed |
| `openspec validate --all --strict` | 1 | See below |

The strict run fails only on the pre-existing open changes `add-sender-notification-replies` (3 warnings) and `complete-mobile-notification-results` (2 warnings). Every warning is "Requirement text is very long". This change does not touch them, so they are recorded as a follow-up. No other open change modifies `notification-capture`.

## Other checks

`git diff --check`: no whitespace errors.

## Not verified

- **On-device capture, the "Disimpan di ponsel" filter, "Kirim ke Tracker" and the Settings list.** The owner has not instructed an install. The UI was checked only by compiling it and by the instrumented test that was compiled but not run. Accessibility was not checked on a device.
- **Re-post behaviour of real apps.** Whether real apps keep `Notification.when` across re-posts is assumed from platform behaviour, not observed. The device database was not read for this phase.
- **The 2-minute window against real Gojek/GoPay delays.** It is not measured. See the open decisions in `design.md`.
- **Backend interaction with isolated single-row batches.** Only the fake tracker in `BatchIsolationTest` was used. No backend run took place in this phase, and the request contract is unchanged.
