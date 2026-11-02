## 1. Classifier

- [x] 1.1 Add `CaptureClassifier` (D1): the drop rules, the recognised rule (local summary or generic backend rule with a ported `parse_idr_amount`), the keep-on-phone reasons in backend order, and send by default.
  - Evidence: `CaptureClassifierTest` covers the device-sample noise shapes (Gojek promo, Stockbit IHSG/trending/Net Sell, ShopeePay "GAJIAN! s.d.", Jago "up to 30K BONUS"). It also covers the transaction shapes (GoPay "Kamu baru aja bayar", myBCA "You spent IDR 25,000.00", Jago debit card, ShopeePay top-up, Jago pocket moves), the safety-footer cases, and the drop flags. All pass.
- [x] 1.2 Add the institution groups and descriptions to `FinancialSourceRegistry`, and derive the selectable list from it (D9).
  - Evidence: `RegistryTest` asserts one entry per lowercase package, the labels, and the groups.

Expected Result: every capture shape has a tested outcome and reason, with no Android dependency.

## 2. Storage and identity

- [x] 2.1 Add Room schema 6 (D8) with `MIGRATION_5_6`, the exported `6.json`, and the entity fields.
  - Evidence: the Robolectric `CaptureMigrationTest` migrates a populated version 5 database built from `5.json`, Room validates it against version 6, and the row reads back as a send row. A mutation that drops one migration index makes it fail.
- [x] 2.2 Add the DAO capture transaction: the posting-identity update (D5), duplicate pairing (D6), the insert, the outcome filters on the feed, submission and count queries, the local flow, the send-local update and the retention prune.
  - Evidence: `CaptureStoreTest` (Robolectric, in-memory Room) covers re-post update, identical payments at different times, Gojek/GoPay pairing inside and outside 2 minutes, one-to-one pairing, summary restatement, the send-local action, and the 7-day prune.
- [x] 2.3 Rewrite `NotificationProcessor` capture on the serial queue with retry and failure counting (D3). Read text lines, messages, flags and `when` in the listener service (D4). Update the debug receiver.
  - Evidence: `RetryingWriteTest` (retry, single failure report, cancellation), `CaptureProcessorTest` (Robolectric: send row, kept row, two counted drops, failure counter), and the debug build compiles.

Expected Result: captures are stored once per posting, duplicates are linked, and failures are visible.

## 3. Submission

- [x] 3.1 Add `submitIsolating` (D7) and use it in `NotificationSubmission`. Prune kept and dropped rows at the start of each run.
  - Evidence: `BatchIsolationTest` covers a 422 with one bad row, a 503 during isolation, a 401 during isolation, and a single-row 422.

Expected Result: a bad row no longer stalls the others.

## 4. Interface

- [x] 4.1 Inbox: add the "Disimpan di ponsel" filter, the "Di ponsel" badge, and the "Kirim ke Tracker" section in the detail sheet (D9).
  - Evidence: compiles; the behaviour behind the action is covered by `CaptureStoreTest`. On-device UI check pending owner install.
- [x] 4.2 Settings: list every registered package with its toggle and description, and add the failed-capture and dropped counters to listener health.
  - Evidence: compiles; `RegistryTest` covers the list. On-device UI check pending owner install.

Expected Result: the owner can see and send kept rows, toggle every app, and see capture failures.

## 5. Build and delivery

- [x] 5.1 Bump to 1.7.0 (code 14). Run `./gradlew testDebugUnitTest --rerun-tasks`, compile the instrumented tests with `assembleDebugAndroidTest`, then build with `./gradlew clean assembleDebug --no-build-cache`.
  - Evidence: test XML counts and timestamps; dexdump shows `"1.7.0"`.
- [x] 5.2 Validate with `openspec validate add-reliable-capture-classification --strict` and `openspec validate --all`.
  - Evidence: exit status 0.
- [x] 5.3 Write `verification.md`, archive, re-validate, commit with subject-only messages, and push `feat/reliable-capture-classification`.
  - Evidence: archive exit status 0, and the pushed branch.

Expected Result: a verified 1.7.0 debug build and a pushed branch, ready for the owner's review. No device install.
