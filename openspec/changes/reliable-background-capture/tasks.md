# Tasks

## 1. Submission

- [x] 1.1 Extract `NotificationSubmission.submitPending()` with a process-wide mutex and make `NotificationSyncWorker` use it; verify the existing instrumented worker tests pass unchanged. Evidence: on-device suite 20 passed, 1 skipped (includes the three `NotificationWorkerTest` cases through the shared path).
- [x] 1.2 Add `DirectSync` (submit, then poll results at 5/10/20/40/60 s, deliver alerts, repeat once if triggered during a run) and trigger it from `NotificationProcessor` after a new capture; verify with a unit test of the poll schedule (`DirectSyncTest`). Non-overlap relies on the shared `submissionMutex` in `NotificationSubmission` (by construction); no dedicated overlap test was written. Device: direct sync ran on listener reconnect at 07:42 (outcome `nothing`).

## 2. Diagnostics

- [x] 2.1 Record listener connect/disconnect, disconnect count, last capture, and last direct sync in `AppPreferences`; request rebind on disconnect and on app open when disconnected; verify with the instrumented `ListenerHealthTest` and a device check (connect time and direct sync recorded after install).
- [x] 2.2 Show a "Status Listener" card in Settings; verify on the device (screenshot via adb). Evidence: all six rows rendered on the TECNO at 07:44.

## 3. Release

- [ ] 3.1 Build 1.4.0 with `--no-build-cache`, verify the compiled version string, install after saving the 1.3.2 APK, run the on-device suite, and confirm one real capture is submitted and alerted with the app in the background. Progress: 1.4.0 built without cache, version strings verified, 1.3.2 APK saved, installed, on-device suite 20 passed / 1 skipped. Pending: a real capture alerted with the app in the background.
