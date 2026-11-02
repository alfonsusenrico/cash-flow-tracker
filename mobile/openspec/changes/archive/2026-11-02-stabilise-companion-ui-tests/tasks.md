# Tasks

## 1. Reproduce and prove the cause

- [x] 1.1 Run `CompanionShellUiTest` 20 times in UTC and 20 times in Asia/Jakarta, and 15 times under CPU load, on b107e67, and record the failure rate per test from the JUnit XML. Verified: see 3.1.
- [x] 1.2 Experiment E1: delay every Room task by 40 ms. Verified: the three CI and local assertion fingerprints fail on 3 of 3 runs, and no other test fails.
- [x] 1.3 Experiment E2: hold every background query for 40 ms between preparing it and reading its cursor. Verified: `themeChoiceIsStoredAndReadBack` fails with `UncaughtExceptionsBeforeTest` on 3 of 3 runs, caused by "connection pool has been closed" in cancelled view-model flows.
- [x] 1.4 Record the root-cause ledger in design.md. Verified: H1, H2 and H2b confirmed; H3 to H6 rejected with evidence.

## 2. Fix the test harness

- [x] 2.1 Add `RoomIdlingExecutor` and `ShellDatabase` (test sources) and the test-only `espresso-idling-resource` dependency. Verified: `CompanionShellUiTest` and `ShellDatabaseTest` pass.
- [x] 2.2 Move `CompanionShellUiTest` onto `ShellDatabase` without changing any assertion or wait. Verified: `git diff` touches only setup and teardown.
- [x] 2.3 Add `ShellDatabaseTest` over a slow SQLite. Verified: it fails 3 of 3 runs with the idling resource turned off and passes 3 of 3 with it.
- [x] 2.4 Check the E2 matrix with each half of the fix turned off. Verified: idling only leaves `UncaughtExceptionsBeforeTest`; teardown only leaves the closed-pool and assertion failures; both pass 3 of 3.

## 3. Verify

- [x] 3.1 Before (b107e67): class runs 1 of 20 failed in UTC (`deletingFromRincianClosesItAndOffersUndo`), 0 of 20 in Asia/Jakarta, 0 of 15 under CPU load. Full suite: 3 of 3 runs passed (620 tests each). The local rate is too low to show the flake in 3 runs; CI failed 1 of 1.
- [x] 3.2 After: class runs 0 of 20 failed in UTC, 0 of 20 in Asia/Jakarta, 0 of 10 under CPU load (11 tests each).
- [x] 3.3 After: the redesign's other new suites (`HubScreenUiTest`, `EveningReminderTest`, `SelfTestListenerTest`, `HealthReporterTest`, `CaptureTimeTest`, the large-text suites) and `ShellDatabaseTest` (91 tests per run): 0 of 5 runs failed in UTC, 0 of 5 in Asia/Jakarta.
- [x] 3.4 After: full suite 3 of 3 runs passed (621 tests each), and 1 of 1 with `--no-daemon` in UTC as on CI.
- [x] 3.5 After: each of the 11 shell tests run alone: 11 of 11 passed.
- [x] 3.6 `lintDebug` and `assembleDebug` pass; `openspec validate --all --strict` passes in `mobile/` and the root. Verified: `lintDebug` and `assembleDebug` exit 0; both validations exit 0.
