# Tasks

## 1. Contract, storage and queue
- [x] 1.1 Record approvals, the baseline (306 unit tests on 51d852e) and the backend contract at 3310e24. Evidence: proposal.md, design.md, baseline JUnit XML.
- [x] 1.2 Add `text_captures` with the 7→8 migration and exported schema 8. Evidence: `TextCaptureMigrationTest` (rows of every table kept, definitions unchanged, unique client id).
- [x] 1.3 Implement create, claim, send, resend, recovery, answers with reply ids, undo, edit, delete from the tracker, discard and suggestions. Evidence: `TextCapturesTest` (20 mock-tracker cases: offline, 503, 422, 401, not paired, duplicate send, process death, stale claim, 9.9 s/10.1 s, 409, reply-id reuse, 404 delete).
- [x] 1.4 Put the resend under the auto-sync switch and "Sinkron sekarang". Evidence: `AutoSyncTest.theSwitchAlsoGovernsQueuedTypedCaptures`, `TextCapturesTest.autoSyncOffLeavesAnOfflineCaptureForKirim`.

Expected Result: captures are stored before sending, never lost or duplicated, and resent only as the switch allows.

## 2. Screens
- [x] 2.1 Add the raised Catat button and the capture sheet with every state, suggestions, questions, countdown and reduced motion. Evidence: `CaptureSheetUiTest` (14 states × light/dark × font 1.0/2.0), `CaptureFlowUiTest`, `CompanionShellUiTest`.
- [x] 2.2 Show captures in Aktivitas, the home figures and a capture Rincian with "Hapus dari tracker" behind a confirmation. Evidence: `CaptureModelTest`, `CaptureFlowUiTest` (feed, Di ponsel, Rincian, notification Rincian without tracker delete).
- [x] 2.3 Version 2.3.0/22 and README. Evidence: dexdump of the clean APK, README sections 2 and 6.

Expected Result: every approved capture screen works in both themes and at font scale 2.0 without changing notification behaviour.

## 3. Integration and delivery
- [x] 3.1 Synthetic local backend harness with a `_test` database guard, AI refused, loopback only. Evidence: `scripts/verify/capture_backend.py`, Flyway V1–V37 log, `CaptureLocalBackendTest` (6 cases on the JVM against it).
- [x] 3.2 Device tests: local-backend flows, screenshots and the 2.2.0 → 2.3.0 upgrade. Evidence: `CaptureBackendDeviceTest`, `CaptureScreenshotTest`, `CaptureUpgradeDeviceTest` compiled; their runs on the Realme are recorded in verification.md.
- [x] 3.3 Full unit suite, lint, clean APK, OpenSpec strict validation, archive, commit and push. Evidence: verification.md.

Expected Result: a reviewable branch with recorded evidence and the change archived.
