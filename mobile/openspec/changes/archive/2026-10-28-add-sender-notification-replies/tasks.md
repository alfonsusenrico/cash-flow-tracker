# Tasks

Adopted from the owner-approved coordinated backend change `add-notification-sender-clarification` on 2026-10-05. Backend responsibilities remain tracked there.

## 1. Typed sender results and durable storage

**Expected Result:** Sender questions and queued answers remain bound to the correct event and server pairing across restart and upgrade.

- [x] 1.1 Extend API models/result validation for sender and revision-aware account questions; verify malformed/unsupported payloads, correlation, legacy account actions and recorded-result contracts.
- [x] 1.2 Add a non-destructive Room reply outbox migration and retention; verify populated upgrade, duplicate enqueue, restart recovery, backup exclusions and preserved existing rows/receipts/pairing.

## 2. Reply delivery and interaction

**Expected Result:** Inline and in-app answers have durable delivery and honest pending/recorded states.

- [x] 2.1 Add authenticated reply worker, bounded retries and pairing isolation; verify offline/5xx/auth/conflict/validation/lost-response cases and no re-ingestion or premature success.
- [x] 2.2 Add restricted direct reply/no-name notification actions with event/question-specific identities; verify concurrent and sequential questions, stale actions, locked-device authentication/fallback and disabled permissions.
- [x] 2.3 Add equivalent detail-sheet controls, reply state and question-aware alert receipts; verify blank/long-name/error/dismissal cases, enlarged-text/focus/touch labels and one logical recorded alert without a separate TalkBack acceptance gate.

## 3. Verification

**Expected Result:** Tests and a USB/local-backend scenario prove safe reply delivery and storage preservation.

- [x] 3.1 Run unit tests, lint and no-cache APK build; verify actual version constants, manifest restrictions and Room schema.
- [x] 3.2 Run instrumentation for migrations, workers, question UI and routing; verify platform authentication branches and populated upgrade.
- [x] 3.3 Run coordinated USB/local-backend fictional scenarios for name/no-name/future reuse/concurrent questions/sequential questions/offline restart/stale replies/dismissal/pairing; verify one ledger record and unchanged normal-app production pairing.
- [x] 3.4 Run strict native validation, diff/privacy review and update handoff with exact revisions, passes/skips and remaining risks.

## 4. Delivery

**Expected Result:** A verified companion artifact is ready for the backend-first rollout without altering production pairing.

- [x] 4.1 Commit a clean companion topic branch and report artifact/version; verify source identity and leave remote publication unused while this repository has no remote.
- [x] 4.2 After owner merge and successful backend pipeline, perform the authorized USB normal-app upgrade with a rollback APK; verify version, listener and data/config preservation without exposing credentials or personal records.
- [x] 4.3 Present verification for owner acceptance and synchronize/archive accepted specs with native commands; verify requirements reached durable specs and report pending acceptance/release steps.

Archive note (2026-10-28): the owner-approved automation plan (phase 8, "old OpenSpec changes archived") authorizes closing this change. The implementation is on `main`: commit `2ffc420` (`SenderReplies`, `SenderReplyWorker`, `SenderReplyReceiver`, `SenderConfirmationNotifier`, Room `MIGRATION_3_4`), with `SenderQuestionsTest`, `SenderReplyPersistenceTest`, `SenderReplyWorkerTest` and `SenderQuestionUiTest`. Later companion releases (1.7.0, 1.8.0) were built on top of it.
