# Verification

Tested 2026-10-30 and 2026-10-31 on branch `feat/text-capture` from mobile main 51d852e (2.2.0, code 21, Room 7), with the uncommitted phase 3 working tree that this change commits. The backend contract and local backend come from cash-flow-tracker 3310e24, exported with `git archive` into a scratch folder. origin/main moved to 02f4468 during the work; the capture router, suggestions and resolution are identical there, and `text_capture.py` only adds plan linking after a write. All data is synthetic.

## Automated checks
- **Baseline:** `./gradlew testDebugUnitTest --rerun-tasks` on 51d852e: 306 tests, 0 failures, 0 errors, 0 skipped (JUnit XML).
- **After:** the same command on the final tree: 415 tests, 409 passed, 0 failures, 0 errors, 6 skipped. The 6 skipped are `CaptureLocalBackendTest`, which runs only with `CAPTURE_LOCAL_BACKEND` set (below). 103 new tests run without it:
  - `TextCaptureMigrationTest` (2): a populated v7 database keeps its notification, reply and receipt rows; every existing table and index definition is unchanged; only `text_captures` and its two indices are added; a repeated client id is refused.
  - `TextCapturesTest` (20, fictional tracker): stored before any request; recorded, needs-detail and undone mapping; answer reply-id reuse across a lost response and a resend; local and server answer refusal; stale question reload; offline queue resent once with the same client id; auto-sync off waits for Kirim; concurrent sends make one request; process death mid-send recovered without loss or duplicate; stale claim taken over; 503/422/401/not-paired kept apart; undo at 9.9 s sent and at 10.1 s not sent; 409 `undo_expired` and `capture_transaction_changed`; deadline from the server clock; Hapus dari tracker for the capture's own expense, 404, moves refused, offline unchanged; edit ends the undo window; discard rules; suggestions query encodes `+07:00`.
  - `CaptureModelTest` (10): text bounds, state mapping, undo window edges, the sheet state machine for queued, sending, recorded, saved, needs_detail, undone, failed and deleted, delete and discard abilities, presentation, home figures and filters.
  - `CaptureSheetUiTest` (56): 14 sheet states in light and dark at font scale 1.0 and 2.0, each with its copy and 48 dp controls.
  - `CaptureFlowUiTest` (12): a suggestion fills and focuses the input and never sends; empty and unavailable suggestions; draining bar present, and removed under reduced motion; chips and "Rekening lain" answers; typed amount; capture Rincian with Hapus dari tracker behind a confirmation; moves without it; notification Rincian with only Hapus dari aplikasi; feed and Di ponsel.
  - `CompanionShellUiTest` (+2): Catat opens the sheet; a capture without a tracker waits in the queue and "Lihat antrean" opens Di ponsel; closing returns to the tab. `AutoSyncTest` (+1): the switch governs the capture resend and Kirim queues it while off.
- **Lint:** `lintDebug` on the final tree: 0 errors, 50 warnings, none in the new capture code (dependency versions, unused legacy resources, icons, the existing battery-exemption and debug network notes).
- **APK:** `./gradlew clean assembleDebug --no-build-cache` with the default server. `aapt2` reports `versionCode='22' versionName='2.3.0'`; dexdump shows the strings `"2.3.0"` and `"https://finance.alfonsusenrico.com"` and no loopback address. SHA-256 a1835d5de66e1730e2f7f1f808f55e5a383b3f0af1c6183a524592b25a768e75 (a copy is kept in the scratch folder, not installed anywhere).
- **OpenSpec:** `openspec validate --all --strict`: 9 passed, 0 failed, exit 0.
- After `openspec archive add-text-capture --yes`, strict validation flagged the merged auto-sync requirement as longer than 500 characters; its wording was shortened without changing its meaning or any scenario name, and `openspec validate --all --strict` then passed 9 of 9 with exit 0.
- `git diff --check`: clean.

## Local backend
- Disposable `postgres:16` container `cft-p3-db` on 127.0.0.1:55591, database `cft_capture_test`, migrated V1–V37 with `flyway/flyway:10` ("Successfully applied 37 migrations … now at version v37").
- `scripts/verify/capture_backend.py` served the 3310e24 export on 127.0.0.1:18093 with AI disabled (no provider; the harness refuses to start with one) and two fictional owners keyed `fictional-capture-key` (history) and `fictional-empty-key`. A curl smoke check returned one history suggestion, an empty list for the second owner, and an account question for an unknown phrase.
- `CAPTURE_LOCAL_BACKEND=http://127.0.0.1:18093 ./gradlew testDebugUnitTest --tests '*CaptureLocalBackendTest'`: 6 passed, 0 failed (18.6 s). This is the real client code on the JVM, not the phone: suggestions present and absent; a confident capture recorded with `source = capture`; an unsure capture asked account then category and recorded; undo within the window removed the transaction (404 afterwards); after 16 s the phone refused undo locally, the backend answered 409 `undo_expired`, and Hapus dari tracker deleted the transaction (404 afterwards); an offline capture queued and was then sent once, and a repeat of its client id returned the same capture and transaction.
- The fixture process, the container and the `cft-p3-net` network were stopped and removed. No `adb reverse` mapping was created.

## Device checks: not run
No Android device was attached during the work: `adb devices -l` listed none at every check (2026-10-30 and repeatedly on 2026-10-31). The test phone (Realme RMX1921) was never reached, nothing was installed or uninstalled on any phone, and no adb command other than `adb devices -l` was issued. The TECNO was not touched. These device items are therefore not run:
- `CaptureBackendDeviceTest` (confident capture with undo, unsure capture with questions, delete after the window, suggestions present and absent, offline then Kirim) through `adb reverse tcp:18093`;
- `CaptureScreenshotTest` in light, dark and font scale 2.0, and the font scale restore;
- the 2.2.0 → 2.3.0 upgrade with `SyntheticDeviceSeedTest` and `CaptureUpgradeDeviceTest`.

The three test classes compile with `-PcompanionVerification` and README section 6 gives the commands. Touch delivery, IME insets and real TalkBack speech on the sheet therefore remain unverified on hardware.
