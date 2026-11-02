# Tasks

## 1. Contract and persistence

Expected result: typed questions and answers preserve existing sender work during upgrade.

- [x] 1.1 Add source-pocket network/result validation and notification routing; verify malformed/unknown questions cannot create recorded alerts and sender/account results still validate.
- [x] 1.2 Migrate Room 4→5 with typed account answers and cached question-bound options; verify existing notifications, receipts, pending sender rows and scheduled worker compatibility.

## 2. Interaction and delivery

Expected result: explicit selection survives offline/restart and uses authoritative backend completion.

- [x] 2.1 Add the notification action and inbox chooser with full eligible options, no selection, loading/error/refresh/cached states and accessible existing-theme controls; verify UI tests, light/dark and enlarged text.
- [x] 2.2 Extend durable delivery with typed pocket payloads and shared sender recovery; verify duplicate, auth, pairing, stale, invalid, retry and response-identity tests.

## 3. Verification and delivery

Expected result: fictional USB/local-backend evidence and a clean versioned artifact are ready for owner release.

- [x] 3.1 Run unit/instrumentation/lint/build, native strict validation and diff checks; verify fictional USB end-to-end tests on TECNO only and document limits.
- [x] 3.2 Build 1.6.0/code 12, preserve rollback and commit the clean topic branch; verify artifact/signing identity and no production configuration overwrite.
- [x] 3.3 After exact-revision backend deployment and install authorization, upgrade normal app preserving pairing and rows; verify value-free installed metadata and listener status, then await owner acceptance before sync/archive.

Verification transport: owner-reconnected TECNO hardware serial verified; wireless ADB used after USB disappeared. USB transport remains unverified. See docs/jago-pocket-verification.md for executed evidence.

2026-10-06: normal 1.6.0/code 12 installed at explicit owner request while backend deployment runs. The on-device value-free check proves Room 5 and preserved production pairing, 98 records and 43 receipts; listener foreground/system-bound. Installation portion of 3.3 complete; exact-revision GitHub deployment succeeded in run 37424356067. Owner acceptance/sync/archive remains pending. See ignored installed-release.json for receipt.

Archive note (2026-10-28): the owner-approved automation plan (phase 8, "old OpenSpec changes archived") authorizes closing this change. The implementation is on `main`: commit `ed864e8`, merged in PR #1 (`SourcePocketQuestions`, `SourcePocketReplies`, `SourcePocketConfirmationNotifier`, `SourcePocketQuestionControls`, Room `MIGRATION_4_5`), with `SourcePocketQuestionsTest`, `SourcePocketQuestionUiTest` and `SourcePocketReplyPersistenceTest`. Later companion releases (1.7.0, 1.8.0) were built on top of it.
