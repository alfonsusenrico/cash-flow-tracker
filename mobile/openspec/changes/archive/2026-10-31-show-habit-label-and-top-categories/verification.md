# Verification

All checks ran on 2026-10-31 in the worktree on branch `feat/habit-label-top-categories`, from 2cd2572 with this change applied.

## Automated checks
- **Baseline** on 2cd2572, `./gradlew testDebugUnitTest --rerun-tasks`, counted from the JUnit XML: 498 tests, 0 failures, 0 errors, 6 skipped (the `CaptureLocalBackendTest` cases, which need a local backend).
- **After**, same command and count: 565 tests, 0 failures, 0 errors, 6 skipped (the same six). Added 67:
  - `TrackerOptionalFieldsTest` 9 (habit with, without and incomplete; habit round trip through the stored JSON; Rincian line; top categories with, without, empty, seven contract breaks; round trip keeping absent and empty apart).
  - `HabitResultTest` 3 (a habit result is stored in `result_json` and shown; an older tracker leaves no habit; a later result without the habit removes it).
  - `HabitLabelUiTest` 16 (4 tests × light/dark × font 1.0/2.0: only the habit row is tagged; TalkBack "Warung Contoh, Keluar Rp27.000, Tercatat, Dari kebiasaan, GoPay Contoh, 12.10"; Rincian line; no line on an ordinary row).
  - `TopCategoriesUiTest` 36 (9 tests × light/dark × font 1.0/2.0: top three with pillar, period and freshness; TalkBack per category; every Beranda amount is the tracker's; no tile for an absent field, a broken list or no summary; empty state without Rp0; stale and restored freshness).
  - `LedgerSummaryRefreshTest` 3 (categories cached, restored and kept stale with the summary; replaced by an empty or absent list; dropped on re-pairing), against MockWebServer.
  - `ContrastTest` gains four bar pairs; it is still one test and passes.
- The first run of `TopCategoriesUiTest` failed its empty-state case (`NoSuchElementException` from taking the largest of an empty list); the tile now handles the empty list first.
- `./gradlew lintDebug`: exit 0. No issue in a changed file; `app/build.gradle.kts` has only the existing dependency-version warnings on lines this change did not touch.
- `./gradlew compileDebugAndroidTestKotlin -PcompanionVerification`: exit 0.
- `./gradlew clean assembleDebug --no-build-cache`: exit 0. `aapt2 dump badging`: `com.alfonsusenrico.financialtracker.listener.debug`, versionCode 25, versionName 2.3.3. `dexdump` of `BuildConfig`: `VERSION_CODE` 25, `VERSION_NAME` "2.3.3", and `<clinit>` loads `const-string "https://finance.alfonsusenrico.com"` for `SERVER_URL`.

## Local backend
Not run. `git ls-remote origin refs/heads/chore/small-follow-ups` in cash-flow-tracker returned nothing on 2026-10-31, and origin/main 26e04af has no `top_categories`. The summary contract is covered by MockWebServer in `LedgerSummaryRefreshTest`; the habit contract by the backend's own `compact_result` shape read at 26e04af.

## Visual check (JVM, not a device)
A throwaway Robolectric native-graphics render (deleted before commit) drew Beranda, Aktivitas and Rincian with synthetic fixtures in light and dark at font 1.0 and 2.0, 360 dp wide. Files: the coordinator's scratch folder `small-mobile/jvm-render/`. The tile, tag and Rincian line render whole in all four. The same renders show two pre-existing wraps at font 2.0 (design.md, open decision 4).

## Device checks
Not run. `adb devices -l` listed only the owner's phone (TECNO); the test phone (Realme) was not attached. Nothing was installed, no setting was changed, no input was sent, and the TECNO was not addressed by any command.

## OpenSpec
- `openspec validate --all --strict` before archiving: exit 0, 10 passed, 0 failed.
- `openspec archive show-habit-label-and-top-categories --yes`: `companion-interface` +1 added, ~3 modified.
- `openspec validate --all --strict` after archiving: exit 0, 9 passed, 0 failed.

## Large text follow-up (coordinator, 2026-10-31, same branch, still 2.3.3/25)
- `LargeTextAmountsUiTest` (8 cases: Beranda and Aktivitas × 360/320 dp × light/dark, font 2.0, Robolectric native graphics). With the main code of cbf29f8 restored (`git stash` of the three layout files) all 8 failed: Beranda "Rp50.000 in \"Rp50.000\" stays on one line expected:<0> but was:<3>" (360 dp) and "Rp690.000 … expected:<0> but was:<1>" (320 dp); Aktivitas "\"HARI INI · KAMIS, 29 OKT\" takes 5 lines" (360 dp) and "takes 13 lines" (320 dp). With the fix all 8 pass.
- `./gradlew testDebugUnitTest --rerun-tasks`, counted from the JUnit XML: 573 tests, 0 failures, 0 errors, 6 skipped (the same `CaptureLocalBackendTest` cases). That is 565 plus the 8 new cases.
- `./gradlew lintDebug`: exit 0, no issue in a changed file. `compileDebugAndroidTestKotlin -PcompanionVerification`: exit 0.
- `./gradlew clean assembleDebug --no-build-cache`: exit 0; `aapt2` versionCode 25, versionName 2.3.3; `dexdump` of `BuildConfig`: `VERSION_CODE` 25, `VERSION_NAME` "2.3.3", `const-string "https://finance.alfonsusenrico.com"`.
- JVM renders (throwaway native-graphics test, deleted before commit) of Beranda and Aktivitas in light and dark at font 1.0 and 2.0: the coordinator's scratch folder `small-mobile/jvm-render-2/`. At 2.0 "Dikurangi penggantian" is above "Rp50.000"; day headers read "HARI INI ·" / "KAMIS, 29 OKT" / "KELUAR RP246.500". At 1.0 both layouts are unchanged.
- Device checks: still not run. `adb devices -l` again listed only the TECNO; no other adb command was sent and the TECNO was not addressed.
- `openspec validate --all --strict` after adding the scenario to the main spec and this archived delta: exit 0, 9 passed, 0 failed.
