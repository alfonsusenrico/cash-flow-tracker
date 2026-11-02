# Design

## Context
See proposal.md for why. `CompanionShellUiTest` is the only Compose UI suite that runs the real shell over a real (in-memory) Room database and real view models. Every other UI suite (`HubScreenUiTest`, `CompanionScreensUiTest`, the large-text suites and others) renders from fixture lists in memory, and the non-UI suites from the redesign (`EveningReminderTest`, `SelfTestListenerTest`, `HealthReporterTest`, `CaptureTimeTest`) use fake schedulers, injected clocks and explicit zones.

Robolectric runs these tests on the JVM in paused-looper mode. Compose's waits and assertions idle the main looper and the composition, through `Espresso.onIdle()`. Room runs queries, flow updates and invalidation refreshes on its own thread pool, which nothing in that loop sees.

## Failure fingerprint
- **Expected:** the shell suite passes on every run, on CI (ubuntu, Temurin 17, `--no-daemon`, UTC) and locally in any zone.
- **Observed on CI, b107e67:** 1 of 620 tests failed, `questionNotificationOpensTheQuestionSheetAndOthersOpenRincian`, at `CompanionShellUiTest.kt:147` (`assertExists` on "1 dari 2 menunggu").
- **Observed locally by the coordinator** (class only, `--rerun-tasks`): with UTC, `themeChoiceIsStoredAndReadBack` failed with `UncaughtExceptionsBeforeTest`. With Asia/Jakarta, it failed the same way, together with `theEveningReminderOpensTheQuestionSheet` ("Perlu sekarang" not found) and `deletingFromRincianClosesItAndOffersUndo` ("Hapus dari aplikasi" not found for a click).
- **Revision:** b107e67, Robolectric 4.14.1, Compose ui-test 1.7.0, kotlinx-coroutines-test 1.8.1, Room 2.6.1.

## Root-cause ledger

| ID | Mechanism | Supporting evidence | Conflicting evidence | Prediction that must hold | Discriminating experiment | Result |
| --- | --- | --- | --- | --- | --- | --- |
| H1 | A test asserts content that a sheet loads from Room. The test waits for the sheet heading, which renders at once, and then asserts the content without waiting for it. Compose's idling does not cover Room's threads. | All three assertion failures follow `waitFor(heading)` or a click, and each target needs a row from `viewModel.row(id)` (`collectAsState(initial = null)`). | It fails rarely: 1 in 20 class runs locally in UTC, 0 in 20 in Asia/Jakarta, 0 in 15 under CPU load. | If Room is slowed down, exactly these assertions fail on every run. | E1: Room's query executor delays each task by 40 ms. | **Confirmed.** 3 of 3 runs failed exactly the three fingerprints: lines 147, 159 and 207 (shifted by the patch). The other 8 tests passed. |
| H2 | Teardown closes the database while Room work for the test is still running. A cancelled view-model coroutine still finishes the query it started, and then fails on the closed pool. `viewModelScope` has a `SupervisorJob` and no handler, so the failure reaches the global handler after the test. kotlinx-coroutines-test stores it and fails the next test with `UncaughtExceptionsBeforeTest`. | `themeChoiceIsStoredAndReadBack` is always the victim, and its predecessor in JUnit's fixed order is the Catat test. The old `@After` only cancelled the view models before `db.close()`. | Not reproduced in 55 unmodified runs. | Holding each query between preparing it and reading its cursor makes the next test fail, with an `IllegalStateException` from a Room flow in a cancelled `StandaloneCoroutine`. | E2: an `openHelperFactory` that sleeps 40 ms after `query()` on background threads. | **Confirmed.** 3 of 3 runs failed `themeChoiceIsStoredAndReadBack` with `UncaughtExceptionsBeforeTest`. The suppressed cause was "Cannot perform this operation because the connection pool has been closed" at `cursor.moveToFirst()`, in `NotificationDao`/`AnswerAttemptDao` flows under `StandaloneCoroutine{Cancelled}, Dispatchers.Main.immediate`. |
| H2b | Screens still collecting a row (`collectAsState` of `observeById`) are disposed only after `@After`, so their queries can also run into the closed database. | Seen under E2 with the Room idling resource turned off: the exception came from `produceState`/`collectAsState`. | None. | Waiting for the view-model jobs alone is not enough; Room must also be idle before closing. | E2 with idling off and teardown on. | **Confirmed.** 3 of 3 runs had the `IllegalStateException` thrown from `collectAsState`. |
| H3 | Dependence on the time zone or the real clock (20.00 reminder, "today" counts, the quiet day). | The coordinator's Jakarta run failed more tests. | The shell's `now` is injected, fixtures use `ZoneId.systemDefault()` consistently, and the failing texts do not depend on time. E1 and E2 reproduce the failures in UTC alone. | Failures follow the zone. | Same class, 20 runs per zone. | **Rejected.** UTC failed 1 of 20 and Jakarta 0 of 20. Every fingerprint reproduces in UTC under E1 and E2. |
| H4 | Shared static or singleton state leaks between tests (AppPreferences, the database instance, fakes). | `AppPreferences` is real SharedPreferences. | Each test builds its own in-memory database, and the theme preference is reset in `@After`. The victim fails before its body runs, and the E2 suppressed causes are the predecessor's Room flows. | Each test passes alone and in the class once H1 and H2 are fixed. | Run each of the 11 tests alone after the fix. | **Rejected** as a cause. See tasks.md 3.5. |
| H5 | WorkManager, the scheduler or the network leak work between tests. | The view models can call `SyncScheduler`. | The fixtures are unpaired (`isConfigured` is false), so no network or WorkManager path runs. The suppressed causes are only Room flows. | `WorkManager` or OkHttp frames appear in the leaked exceptions. | Read the E2 suppressed traces. | **Rejected.** |
| H6 | The custom sheet overlay intercepts input. | "Failed to inject touch input". | The message says the node was not found, not that it was covered. It reproduces under E1 as missing content (H1). | The node exists but the click misses it. | E1 trace. | **Rejected.** |

**First violated contract:** the shell UI tests assumed that "Compose is idle" means "the screen shows its data". This test harness owns that contract. Room's executor is outside Compose's idling (H1), and teardown closed the database while that executor and the still-composed screens were using it (H2, H2b). No production code violates a contract: the app never closes its database, and a sheet that renders its frame before its row arrives is correct.

## Decisions
- **Room's executor is an Espresso idling resource (`RoomIdlingExecutor`), not per-assertion waits.** Under Robolectric, every Compose wait, assertion and injected input goes through `Espresso.onIdle()`. Robolectric's `LocalUiController` loops the main looper until each registered resource has signalled idle and the looper is empty. Because the busy count drops only after a task has posted its result to the main thread, a passing assertion means the row has arrived. Rejected alternatives:
  - Adding `waitFor(content)` before each assertion fixes only today's three lines and leaves the trap for the next test.
  - Compose's own `registerIdlingResource` polls on `Dispatchers.Main` with `delay`, which does not advance under Robolectric's paused clock.
  - A same-thread executor would run Room on the main thread, against its threading model and with a risk of deadlock in `withTransaction` and `runBlocking` test calls.
- **`ShellDatabase.close()` cancels the view models, waits with `compose.waitUntil` until their jobs complete and Room is idle, then closes the database and shuts the executor down.** The wait calls `compose.waitForIdle()` inside the condition, because `waitUntil` alone sleeps without running the main looper, where cancelled coroutines finish. A first attempt without that call timed out after 5 s on every test. The executor shutdown waits up to 5 s for its threads, so no Room work outlives its test.
- **The regression test uses a slow SQLite (`SlowSqlite`, 40 ms between preparing a query and reading it), not a slow executor.** This widens the real window that the harness must cover. It fails on every run without the idling resource (3 of 3) and passes with it.
- **The teardown half has no separate permanent test.** In an isolated test class, a query failure after teardown did not surface reliably. The evidence for the teardown half is the E2 matrix on the real suite (below).

## Evidence
Before and after statistics are in tasks.md, section 3. E2 matrix on `CompanionShellUiTest` (3 runs each, 11 tests per run):

| Room idling | Teardown wait | Failures |
| --- | --- | --- |
| off | off (b107e67) | 4 per run: 3 assertions and `UncaughtExceptionsBeforeTest` |
| on | off | 1 per run, `UncaughtExceptionsBeforeTest` (victim after `themeChoiceIsStoredAndReadBack`) |
| off | on | 3 per run: `IllegalStateException` from `collectAsState` and assertions |
| on | on | 0 |

## Risks / Trade-offs
- [A Room task that never finishes would hold every Compose wait] → Robolectric's idling timeout and the 5 s executor shutdown both fail the test, naming the Room executor.
- [The registry is global] → `close()` unregisters the resource in `finally`.
- [Other future suites over a real database might not use the harness] → `ShellDatabase` is the documented way to build one; today only `CompanionShellUiTest` and `ShellDatabaseTest` need it.

## Open decisions for owner
- **A false empty state while a sheet loads (product, not fixed here).** While the row loads (normally a few milliseconds), the Pertanyaan sheet shows "Semua terjawab" and Rincian shows "Item tidak ditemukan", because both treat "not loaded yet" like "none". This did not cause the flakiness, and changing it is a UI change outside this job. The conservative choice is to leave it, and to show nothing (or a neutral loading frame) until the first emission in a later UI change.
