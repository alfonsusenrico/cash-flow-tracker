# Tasks

## 1. Re-read review rows on open

- [x] 1.1 Add `NotificationDao.getRefreshableReviews` and `ReviewResultRefresh` (D1, D3), and call it from `MainActivity.onResume`. Verify: `ReviewResultRefreshTest` `picksRecentReviewAndFailedRowsOfThisPairingOnly`, `leavesOwnerLabelledRowsAndRowsWithUnsentRepliesAlone`, `readsAtMostFiftyNewestFirst`, `nothingIsReadWithoutCredentials`.
- [x] 1.2 Apply refreshed results through the existing path. Verify: `ReviewRefreshWorkerTest` `ignoredOnTheServerBecomesIgnoredAndLeavesTheList`, `recordedOnTheServerBecomesRecordedWithItsResult`, `unchangedReviewStaysAsItIs`.

Expected Result: opening the app brings recent review and failed rows in line with the server.

## 2. Failures

- [x] 2.1 Leave review and failed rows unchanged when a re-read fails (D2). Verify: `failedReadsLeaveTheReviewRowAsItIs` (401, 403, 404, 503).

Expected Result: a failed read never marks a review row `failed` or rewrites its error.

## 3. Release and verification

- [x] 3.1 Set version 1.8.3 (code 18) and the version assertion. Verify: `clean assembleDebug --no-build-cache`, `aapt2` and `dexdump` show 18 and "1.8.3".
- [x] 3.2 Show the new tests fail with the 1.8.2 rules, run the whole unit suite and compile the instrumented tests. Verify: `verification.md`.
- [x] 3.3 Validate. Verify: `openspec validate refresh-stale-review-rows --strict` and `openspec validate --all --strict` exit 0.

Expected Result: a verified 1.8.3 build; no device installation.

## Workflow follow-up

- Archive, re-validate, commit, push `fix/purpose-question-refresh`, report the compare link.
- The owner installs 1.8.3.
