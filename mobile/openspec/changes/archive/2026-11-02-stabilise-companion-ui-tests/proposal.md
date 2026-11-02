# Proposal

## Approval
On 2026-11-02 the owner approved the Phase 0 plan ("ok I approve all you recommend"), which includes shipping the companion redesign as the first signed release, `android-v2.4.0`. The coordinator assigned this job the same day, because the "Android checks" run on `main` b107e67 failed one UI test and blocks that release. This change touches only unit-test code and one test-only dependency; it changes no app behaviour, so it carries no spec delta (`skip_specs: true`).

## Why
The JVM UI tests of the companion shell (`CompanionShellUiTest`) fail now and then: CI on b107e67 failed `questionNotificationOpensTheQuestionSheetAndOthersOpenRincian` at line 147, and local runs failed `theEveningReminderOpensTheQuestionSheet`, `deletingFromRincianClosesItAndOffersUndo` and, with `UncaughtExceptionsBeforeTest`, `themeChoiceIsStoredAndReadBack`. A release must not depend on reruns.

## What Changes
- A test harness, `ShellDatabase`, gives the shell UI tests an in-memory database whose Room executor is an Espresso idling resource, so every Compose wait and assertion also waits for Room to deliver the rows a screen shows.
- Its `close()` cancels the tracked view models, waits until their coroutines have finished and Room is idle, and only then closes the database, so no query fails after its test has ended.
- `CompanionShellUiTest` uses the harness. Its assertions and waits are unchanged.
- A regression test, `ShellDatabaseTest`, runs the shell over a deliberately slow SQLite and checks a sheet's content right after its heading appears.
- `testImplementation("androidx.test.espresso:espresso-idling-resource:3.6.1")`, already on the unit-test runtime through Robolectric, is now also on the compile classpath.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
None. Test infrastructure only.

## Impact
- `mobile/app/src/test/.../ui/ShellDatabase.kt` (new), `ShellDatabaseTest.kt` (new), `CompanionShellUiTest.kt`.
- `mobile/app/build.gradle.kts`: one test-only dependency. The APK, its behaviour and its version are unchanged.
- No production code, API, database schema or release workflow changes.
