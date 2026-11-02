# Tasks

## 1. Reproduce and locate the defect

- [x] 1.1 Add `SenderAnswerPathTest`, which runs each sender answer path through the real database, notifier, receiver and view model beside a same-amount transfer recorded one minute later. Verify: on `d33b39a` the name, unknown, Tanya and neighbour tests pass (H1, H2a, H3 and H5 rejected), and `blankNotificationReplyLeavesTheQuestionAnswerable`, `missingReplyResultsLeaveTheQuestionAnswerable`, `lockedDeviceLeavesTheQuestionAnswerable` and `laterLookupOffersARefusedQuestionAgain` fail (H2b and H2c confirmed). Recorded in `verification.md`.
- [x] 1.2 Record the hypothesis ledger, the first violated contract, and the open device hypotheses in `design.md`. Verify: the ledger table, with H2d and H2e marked inconclusive or not run.

Expected Result: the normal paths are proved to store answers, and the refusal path is proved to leave an unanswerable, unrecorded question.

## 2. Keep refused notification replies answerable

- [x] 2.1 Re-post the question with both actions and the reason after any refused or failed notification attempt, and show the message alone when an answer is stored or the question has closed (D1). Verify: the four tests from 1.1 pass, and `refusedAttemptAfterAStoredAnswerOnlyReportsIt` passes. With `offerAgain` forced back to the old message-only behaviour, the same four tests fail again.

Expected Result: a refused reply never leaves a dead "Konfirmasi pengirim" notification while the question is open.

## 3. Private attempt record and truthful Tanya state

- [x] 3.1 Add `answer_attempts` (Room 9, `MIGRATION_8_9`, exported schema 9) and `AnswerAttempts.record`, which keeps the newest 200 rows (D2, D4). Verify: `AnswerAttemptMigrationTest` (a populated v8 database keeps every row and definition and gains only the table and index), `attemptRecordKeepsOnlyTheNewestTwoHundred`, and the outcome assertions in `SenderAnswerPathTest` (`received`/`queued`, `invalid_name`, `missing_input`, `locked`, `already_answered`, `malformed`, and app `queued`), including the check that the record never holds the name. `TextCaptureMigrationTest` expects the two new definitions.
- [x] 3.2 Show the unsaved-reply notice on Tanya's sender card from the newest settled attempt (D3). Verify: `CompanionScreensUiTest` `senderQuestionSaysANotificationReplyWasNotSaved` and `senderQuestionDropsTheNoticeOnceANewerAttemptSettles`, in both themes.
- [x] 3.3 Pass the channel through every `SenderReplies.enqueue` caller, including the instrumented tests. Verify: `./gradlew -PcompanionVerification compileDebugAndroidTestKotlin` succeeds.

Expected Result: the next lost answer can be located from a copy of the phone's database, and the owner sees in Tanya that a notification reply was not saved.

## 4. Release and verification

- [x] 4.1 Set version 2.3.2 (code 24). Verify: `clean assembleDebug --no-build-cache`, `aapt2 dump badging` shows `versionCode='24' versionName='2.3.2'`, and `dexdump` shows "2.3.2" in `ApiClient`, `NotificationProcessor` and `AnswerAttempts`, plus the production URL.
- [x] 4.2 Run the whole unit suite. Verify: the XML counts in `verification.md` (baseline 452, after 468, 0 failures).
- [x] 4.3 Validate. Verify: `openspec validate fix-lost-sender-answers --strict` and `openspec validate --all --strict` exit 0.

Expected Result: a verified 2.3.2 build with recorded evidence. Device checks are not run because the Realme is absent.

## Workflow follow-up

- Archive with `openspec archive fix-lost-sender-answers --yes`, re-validate all specs, commit, and push `fix/sender-answer-persistence`.
- When the Realme is attached, run the open device experiments (H2e upgrade action, H2d freeze) with the `.verification` app id only.
- The owner installs 2.3.2. After the next lost answer, read `answer_attempts` for its event.

## Follow-up

- `keep-notification-answers-answerable` (tasks 1–3 there) extends the fix to the purpose and account notification answer paths, using the same tests and shared code.
