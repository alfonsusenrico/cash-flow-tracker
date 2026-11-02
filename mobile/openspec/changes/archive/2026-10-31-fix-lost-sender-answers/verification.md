# Verification: fix-lost-sender-answers

Date: 2026-10-31

## Revision

- **Base:** `d33b39a` (`origin/main`, 2.3.1, code 23, Room 8).
- **Branch:** `fix/sender-answer-persistence`. The results below come from the uncommitted working
  tree that the feature commit records.
- **Environment:** JDK 17 (`/opt/homebrew/opt/openjdk@17`), local `ANDROID_HOME` (no
  `local.properties`), Robolectric 4.14.1 with SDK 34, WorkManager test driver 2.9.1, and the
  latest installed build-tools for `aapt2` and `dexdump`. OpenSpec CLI at `/opt/homebrew/bin/openspec`.
- No production system, backend or LLM was contacted. All fixtures are fictional.

## Device

Not run. `adb devices -l` listed only the owner's phone (TECNO). The test phone
(Realme) was not attached. No other adb command was issued, nothing was installed, and no input
event was sent to any device.

## Defect reproduced on the base

`SenderAnswerPathTest` was added to the unmodified `d33b39a` sources and run alone:

| Test | Result on `d33b39a` |
| --- | --- |
| `recordedNeighbourLeavesTheQuestionAndItsOwnActions` | pass |
| `notificationNameIsStoredForItsOwnQuestion` | pass |
| `notificationUnknownIsStoredForItsOwnQuestion` | pass |
| `tanyaNameIsStoredForItsOwnQuestion` | pass |
| `blankNotificationReplyLeavesTheQuestionAnswerable` | **fail**: expected 2 actions, was 0 |
| `missingReplyResultsLeaveTheQuestionAnswerable` | **fail**: expected 2 actions, was 0 |
| `lockedDeviceLeavesTheQuestionAnswerable` | **fail**: expected 2 actions, was 0 |
| `laterLookupOffersARefusedQuestionAgain` | **fail**: expected 2 actions, was 0 |

## Regression tests fail for the expected reason

With the fix in place, `SenderReplies.offerAgain` was temporarily forced back to the
message-only behaviour (`reposted = false`). The same four tests failed again with
"expected:<2> but was:<0>". The other seven `SenderAnswerPathTest` tests passed. This was repeated on the final tree. The file was then restored, checked with `cmp`, and
`SenderAnswerPathTest` passed again.

## Unit suite (XML results under `app/build/test-results/testDebugUnitTest`)

| State | Tests | Failures | Errors | Skipped |
| --- | --- | --- | --- | --- |
| `d33b39a` baseline, `testDebugUnitTest --rerun-tasks` | 452 | 0 | 0 | 6 |
| Working tree 2.3.2, `testDebugUnitTest --rerun-tasks` | 468 | 0 | 0 | 6 |

The 16 new tests are `SenderAnswerPathTest` (11), `AnswerAttemptMigrationTest` (1) and two
`CompanionScreensUiTest` cases run in both themes (4). `TextCaptureMigrationTest` now expects the
version 9 table and index as well.

## Instrumented tests

`./gradlew -PcompanionVerification compileDebugAndroidTestKotlin` succeeded. The tests were not
run, because no permitted device was attached.

## Build identity

`./gradlew clean assembleDebug --no-build-cache` succeeded.

- `aapt2 dump badging`: `package: name='com.alfonsusenrico.financialtracker.listener.debug'
  versionCode='24' versionName='2.3.2'`.
- `dexdump`: `const-string "2.3.2"` appears in `ApiClient` (the `X-Companion-Version`
  interceptor), `NotificationProcessor.toEntity` and `AnswerAttempts.record`. `const-string
  "https://finance.alfonsusenrico.com"` appears in `BuildConfig.<clinit>` and `AppPreferences.<init>`. The only other 2.3.x string,
  "2.3.3", belongs to `com.google.android.datatransport.cct.CctTransportBackend`, which is
  unrelated.

## OpenSpec

`openspec validate fix-lost-sender-answers --strict` and `openspec validate --all --strict` exit 0.

## Follow-up

After `keep-notification-answers-answerable`, the same branch has 498 unit tests, 0 failures and
6 skipped. `SenderAnswerPathTest` (11) still passes. The clean 2.3.2/24 build was re-checked with
dexdump. Evidence is in that change's `verification.md`.
