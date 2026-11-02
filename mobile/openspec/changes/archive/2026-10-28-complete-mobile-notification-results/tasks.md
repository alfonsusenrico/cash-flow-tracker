# Tasks

## 1. Contract and repository setup

- [x] 1.1 Confirm the backend compact result and owner-scoped lookup contract, including optional ordinary `transaction_id`, against isolated synthetic acceptance-to-result evidence; record the active AI quality gate separately before companion integration.
- [x] 1.2 Record the new companion branch and native OpenSpec plan in a local ignored handoff; verify the previous commit/worktree and existing device data are preserved before source edits.

## 2. Persistent completion state

- [x] 2.1 Add explicit Room migrations and exported schemas for event status, pairing context, ordinary edit identity, and logical alert receipts; verify populated v1/v2 migration tests preserve rows and do not retroactively alert or replay historical synced rows.
- [x] 2.2 Persist each batch result transactionally by matching payload hash, including partial/reordered responses, and retain unresolved rows and independent alert receipts; verify missing results, changed pairing, and more than fifteen queued/review records.

## 3. Completion recovery

- [x] 3.1 Add compact result/event/error/transaction-identity models and authenticated GET lookup through existing Retrofit configuration; verify serialization, all statuses, missing optional fields, malformed recorded responses, and safe error handling.
- [x] 3.2 Coordinate non-cancelling bounded upload work and persisted due completion polling; verify arrival during upload, queued-to-recorded, offline/restart, more than fifty events, cancellation, 429/5xx, auth/not-found suspension, and no automatic terminal replay.

## 4. Alerts and controls

- [x] 4.1 Claim alerts by logical record key, validate complete recorded results, and post stable correctly addressed notifications; verify paired movements/redelivery, silent updates, dismissed alerts, permission/channel denial, and the OS-submission crash window.
- [x] 4.2 Align status and edit controls with committed state and actual ordinary UUIDs; verify queued/review/recorded/error/disabled states, no logical-key or movement-leg PATCH, and truthful local label behavior with focused Compose tests.

## 5. Privacy and debug boundary

- [x] 5.1 Remove private payload/error-body logging, restrict debug simulation/purge to app-local test paths, and exclude real preference/database paths from cloud/device backup; verify synthetic logs, merged release/debug manifests, and XML paths.

## 6. Device verification and delivery

- [x] 6.1 Run unit and populated migration tests, instrumentation tests, lint, debug build, and release manifest verification with JDK 17; verify this revision and review the diff for secrets and destructive fallback.
- [x] 6.2 Install the tested debug APK with a data-preserving update on the connected TECNO device; verify stored rows/checksums, pairing settings, listener/notification permissions, status controls, and automated increased-font behavior. The owner explicitly declined TalkBack acceptance, so no TalkBack device exercise is required.
- [x] 6.3 Use a disposable local pairing and fictional events to verify acceptance-to-completion alerts, one active movement alert, offline/restart recovery, permission denial, and ordinary edit identity; verify the original pairing is restored without ingesting existing owner messages.
- [x] 6.4 Update both handoffs and verification evidence, validate native OpenSpec changes, and run diff/security checks; report backend gate, mobile/device result, spend limits, and any remaining risk before release or archive.

Archive note (2026-10-28): the owner-approved automation plan (phase 8, "old OpenSpec changes archived") authorizes closing this change. The implementation is on `main`: commit `7bf48c9` (`NotificationCompletionWorker`, `NotificationResults`, `AlertReceiptEntity`, Room `MIGRATION_1_3`/`MIGRATION_2_3`, backup exclusions), with `NotificationResultContractTest`, `CompletionMigrationTest`, `CompletionPersistenceTest` and `NotificationWorkerTest`. Later companion releases (1.7.0, 1.8.0) were built on top of it.
