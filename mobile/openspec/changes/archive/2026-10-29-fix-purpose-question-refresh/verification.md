# Verification: fix-purpose-question-refresh

Date: 2026-10-29

## Revision

- **Base:** `dc95894` (`origin/main`, 1.8.1, code 16, Room 7).
- **Branch:** `fix/purpose-question-refresh`. The results below come from the uncommitted working
  tree that the feature commit records.
- **Environment:** JDK 17 (`/opt/homebrew/opt/openjdk@17`), `ANDROID_HOME` set to the local SDK
  (no `local.properties`), build-tools 37.0.0 for `aapt2` and `dexdump`, OpenSpec CLI at
  `/opt/homebrew/bin/openspec`, Robolectric 4.14.1 with SDK 34, MockWebServer 4.12.0, WorkManager
  test driver 2.9.1.
- No device was used and nothing was installed. No production system or backend was contacted;
  every backend response in the tests is a fictional fixture.

## Defect confirmed on the base

- `NotificationResults.consume` set `nextAttempt = 0` and `nextCheckAt = null` for every status
  other than `queued` and `processing`. A `recorded` row with an open purpose question and a
  `needs_confirmation` row were therefore never checked again.
- `NotificationCompletionWorker` returned early for any row that was not `queued` or `processing`
  unless it ran as a refresh, and its failure handler set a terminal failure to `failed`.

## New tests fail on the 1.8.1 rules

The fixed sources were copied aside, and these 1.8.1 behaviours were restored in place: no
follow-up for recorded or confirmation rows (the `questionKey` branch disabled), no clearing of
cached source-pocket choices, no settling of unsent answers, failures through the old `defer`, no
`reviewed` rule, and no foreground check. `QuestionRefreshTest`, `QuestionFollowUpWorkerTest` and
`PurposeAnswersTest` were then run:

| Class | Tests | Failed on 1.8.1 rules |
| --- | --- | --- |
| `QuestionRefreshTest` | 14 | 12 |
| `QuestionFollowUpWorkerTest` | 6 | 2 |
| `PurposeAnswersTest` | 10 | 0 |

Failing: `recordedQuestionSchedulesAFollowUpCheck`, `questionStillOpenTakesTheNextBackoffStep`,
`differentQuestionStartsTheScheduleOver`, `checksStopSevenDaysAfterCapture`,
`closedQuestionLeavesTheListAndQuietlyReplacesThePrompt`,
`reviewedTransactionClosesAQuestionTheResultStillCarries`,
`unsentAnswerToAClosedQuestionResolvesClosedWithoutAPost` (the answer stayed `queued`),
`failedCheckKeepsTheRecordAndItsQuestion`, `failedCheckKeepsTheConfirmationPending` (row became
`failed`), `confirmationQuestionIsCheckedOnTheSameSchedule`,
`recordedAfterConfirmationClearsTheQuestionAndItsNotification`,
`foregroundRefreshPicksOnlyOpenQuestionRows`, `stillOpenSchedulesTheNextStep`,
`failedChecksNeverMarkTheRecordFailed` (row became `failed`).

Passing on both: `recordWithoutQuestionSchedulesNothing` and `ignoredAfterConfirmationClearsTheQuestion`
(negative paths), and the worker tests that force a check to run
(`closedOnTheServerClearsTheRowAndReplacesThePromptQuietly`,
`dismissedPromptStaysGoneWhenTheQuestionCloses`, `confirmationAnsweredOnTheServerClearsItsNotification`,
`refreshFindsTheQuestionClosed`). They prove that a check, once it runs, settles the row and its
notification through the existing path; on 1.8.1 no such check was ever scheduled.

The fixed sources were restored afterwards and their SHA-1 checksums matched the copies.

## Unit suite

`./gradlew testDebugUnitTest` after `clean assembleDebug --no-build-cache`: exit 0; JUnit XML
timestamps 2026-10-29T01:38:20Z to 01:38:25Z; 173 tests, 0 failures, 0 errors, 0 skipped across 25
classes. The base had 153; the 20 new tests are in `QuestionRefreshTest` (14) and
`QuestionFollowUpWorkerTest` (6). `PurposeAnswersTest` now expects the header `1.8.2`.

Instrumented tests: `./gradlew compileDebugAndroidTestKotlin` exit 0. They were not run (no device
attached, and the brief forbids installing).

## Build

- `./gradlew clean assembleDebug --no-build-cache`: exit 0.
- `app-debug.apk` SHA-256 `898210e687f7b150e0cc605a6f3441118b79b3276200a40ced405d651bef1709`.
- `aapt2 dump badging`: `com.alfonsusenrico.financialtracker.listener.debug`, `versionCode='17'`,
  `versionName='1.8.2'`.
- `dexdump -f`: `BuildConfig.VERSION_CODE` = 17, `VERSION_NAME` = "1.8.2"; no `1.8.1` string in any
  dex; the strings `question_follow_up_` and `question_closed` are present.

## OpenSpec

- `openspec validate fix-purpose-question-refresh --strict`: exit 0.
- `openspec validate --all --strict`: exit 0, 7 passed, 0 failed, no warnings.
- No other open change modifies the same requirements.

## Other checks

- `git diff --check`: exit 0.
- Room schema: no entity change, `schemas/7.json` unchanged; the new DAO queries compile against it.

## Not verified

- Behaviour on the owner's phone, including how promptly HiOS lets background follow-ups run. The
  foreground check covers the frozen case; the owner must install 1.8.2 for any of this to apply.
- An end-to-end run against a real backend.

## Archive

- `openspec archive fix-purpose-question-refresh --yes`: exit 0; 8 requirements added and 2
  modified across `mobile-notification-completion` and `mobile-purpose-questions`.
- It warned (non-blocking) that the proposal's "Why" section exceeds 1000 characters; it was left
  as written, because it records the owner's report and the added confirmation scope.
- `openspec validate --all --strict` after archiving: exit 0, 6 passed, 0 failed.
