# Verification

All checks ran on 2026-10-31 in the worktree on branch `feat/uncategorised-top-category`, from origin/main 4e0d3e6 with this change applied.

## Automated checks
- **Baseline** on 4e0d3e6 (clean tree), `./gradlew testDebugUnitTest --rerun-tasks`, counted from the JUnit XML: 573 tests, 0 failures, 0 errors, 6 skipped (the `CaptureLocalBackendTest` cases, which need a local backend).
- **New tests on the unchanged main code of 4e0d3e6** (only the test files changed), the three affected classes: 11 failures.
  - `TrackerOptionalFieldsTest`: `anUncategorisedEntryWithoutANameIsCalledTanpaKategori` (a blank name hid the list: "expected … Tanpa kategori … but was:<null>"), `anUncategorisedEntryShowsNoPillarEvenWhenTheTrackerSendsOne` (the pillar was kept), and the "two uncategorised" case of `topCategoriesThatBreakTheContractAreNotShownButTheSummaryStays` (both entries were shown).
  - `TopCategoriesUiTest`: `anUncategorisedEntryWithABlankNameOrAPillarReadsTanpaKategori` and `twoUncategorisedEntriesGetNoTile`, each in all four theme and font cases.
  - Passing already on 4e0d3e6: `anUncategorisedEntryHasANullIdAndKeepsTheTrackersName`, `anUncategorisedEntrySurvivesTheStoredSummaryJson`, `showsTheUncategorisedEntryUnderTheTrackersNameWithoutAPillar`, `talkBackReadsTheUncategorisedEntryAsTanpaKategoriAndItsAmount` and `LedgerSummaryRefreshTest.anUncategorisedEntryIsCachedWithItsNullIdAndRestored`. The old validation never read `category_id`, so the contract's exact entry (name "Tanpa kategori", null pillar) was already shown; the tile was not hidden for it.
- **After**, same command and count on the final tree: 594 tests, 0 failures, 0 errors, 6 skipped (the same six). Added 21:
  - `TrackerOptionalFieldsTest` 4 (null id with the tracker's name; blank, spaces, null and missing name read "Tanpa kategori"; pillar dropped; stored-JSON round trip keeps the null id) and two more cases in the contract-break test (two null ids; an unknown pillar on the null-id entry).
  - `TopCategoriesUiTest` 16 (4 tests × light/dark × font 1.0/2.0: the entry is shown second with Rp150.000 and no pillar; TalkBack reads "Tanpa kategori, Rp150.000"; a blank name with a pillar reads the same; two null ids show no tile and the totals stay).
  - `LedgerSummaryRefreshTest` 1 (the null-id entry is cached from MockWebServer, restored after a new view model with a null id, "Tanpa kategori", no pillar and Rp50.000).
- `./gradlew lintDebug`: exit 0. Its report was removed by the following `clean`, so issues per file were not inspected.
- `./gradlew compileDebugAndroidTestKotlin -PcompanionVerification`: exit 0.
- `./gradlew clean assembleDebug --no-build-cache`: exit 0. `aapt2 dump badging`: `com.alfonsusenrico.financialtracker.listener.debug`, versionCode 26, versionName 2.3.4. `dexdump` of `BuildConfig`: `VERSION_CODE` 26, `VERSION_NAME` "2.3.4", and `<clinit>` loads `const-string "https://finance.alfonsusenrico.com"`.

## Local backend
Not run. At cash-flow-tracker origin/main 15dbf61 `top_categories` still leaves the uncategorised bucket out; the backend change is in parallel. The contract is covered by fixtures and MockWebServer.

## Device checks
Not run. `adb devices -l` listed no device at all, twice; the test phone (Realme) was not attached. Nothing was installed, no setting was changed, no input was sent, and no device was addressed.

## OpenSpec
- `openspec validate --all --strict` before archiving: exit 0, 10 passed, 0 failed.
- `openspec archive show-uncategorised-top-category --yes`: `companion-interface` ~1 modified.
- `openspec validate --all --strict` after archiving first failed (exit 1): strict mode rejected the merged requirement text as longer than 500 characters. The text was shortened to 493 characters in the main spec and this delta, with the naming rule left to the scenario "Spending without a category"; then exit 0, 9 passed, 0 failed.
