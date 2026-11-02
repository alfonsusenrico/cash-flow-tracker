# Tasks

## 1. Follow-up schedule

- [x] 1.1 Add `QuestionRefresh` with the schedule constants and the open-question test (D1), and make `consume` keep one check scheduled for an open purpose question or an open confirmation, stepping on the same question and starting over on a new one. Verify: `QuestionRefreshTest` `recordedQuestionSchedulesAFollowUpCheck`, `questionStillOpenTakesTheNextBackoffStep`, `differentQuestionStartsTheScheduleOver`, `checksStopSevenDaysAfterCapture`, `recordWithoutQuestionSchedulesNothing`, `confirmationQuestionIsCheckedOnTheSameSchedule`.
- [x] 1.2 Add the `question_follow_up_<event>` work, the worker's follow-up mode and its rescheduling (D2). Verify: `QuestionFollowUpWorkerTest` `stillOpenSchedulesTheNextStep` (work enqueued, step 1).

Expected Result: an open question is re-checked at 2 min, 10 min, 30 min, 2 h, 6 h, then every 12 h, never later than 7 days after capture.

## 2. Settling a question closed elsewhere

- [x] 2.1 Keep closing on the existing result path and drop cached source-pocket choices when a row leaves a confirmation (D3). Verify: `QuestionRefreshTest` `closedQuestionLeavesTheListAndQuietlyReplacesThePrompt`, `recordedAfterConfirmationClearsTheQuestionAndItsNotification`, `ignoredAfterConfirmationClearsTheQuestion`; `QuestionFollowUpWorkerTest` `closedOnTheServerClearsTheRowAndReplacesThePromptQuietly`, `dismissedPromptStaysGoneWhenTheQuestionCloses`, `confirmationAnsweredOnTheServerClearsItsNotification`, `refreshFindsTheQuestionClosed`.
- [x] 2.2 Treat `review_status = reviewed` as no open question (D4). Verify: `reviewedTransactionClosesAQuestionTheResultStillCarries`.
- [x] 2.3 Settle unsent purpose answers to a closed question as `closed` / `question_closed` without a POST, excluding the answer being accepted, with its own sheet note (D5). Verify: `unsentAnswerToAClosedQuestionResolvesClosedWithoutAPost`; `PurposeAnswersTest` (10 tests) still passes, including acceptance and 409.

Expected Result: a question answered on the web, by a ledger edit, bulk review, history suggestion, pairing or server-side confirmation disappears from the phone with its notification, and no stale answer is sent.

## 3. Failures and the foreground check

- [x] 3.1 Route failed checks of open rows through `deferFollowUp` (D6). Verify: `failedCheckKeepsTheRecordAndItsQuestion`, `failedCheckKeepsTheConfirmationPending`, `QuestionFollowUpWorkerTest` `failedChecksNeverMarkTheRecordFailed` (503 follow-up and 401 refresh).
- [x] 3.2 Check every open question when the app resumes, regardless of auto-sync (D7). Verify: `foregroundRefreshPicksOnlyOpenQuestionRows` (3 of 7 rows, none without credentials); `MainActivity` compiles with the call.

Expected Result: failed checks never mark a recorded or pending row failed, and opening the app checks open questions at once.

## 4. Release and verification

- [x] 4.1 Set version 1.8.2 (code 17) and the version assertion in `PurposeAnswersTest`. Verify: `clean assembleDebug --no-build-cache`, `aapt2 dump badging` and `dexdump` show `VERSION_CODE` 17 and `VERSION_NAME` "1.8.2".
- [x] 4.2 Show the new tests fail with the 1.8.1 rules restored, then run the whole unit suite and compile the instrumented tests. Verify: `verification.md` (old-rule run, 173 unit tests, `compileDebugAndroidTestKotlin`).
- [x] 4.3 Validate the change and all specs. Verify: `openspec validate fix-purpose-question-refresh --strict` and `openspec validate --all --strict` exit 0.

Expected Result: a verified 1.8.2 build with recorded evidence; no device installation.

## Workflow follow-up

- Archive with `openspec archive fix-purpose-question-refresh --yes`, re-validate all specs, commit, push the branch, and report the compare link.
- The owner installs 1.8.2 on the phone; no backend change is needed.
