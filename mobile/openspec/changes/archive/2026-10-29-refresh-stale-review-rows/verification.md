# Verification: refresh-stale-review-rows

Date: 2026-10-29

## Revision

- **Base:** `efea886` on `fix/purpose-question-refresh` (1.8.2, code 17, Room 7).
- **Branch:** `fix/purpose-question-refresh`. Results come from the uncommitted working tree that
  the feature commit records.
- **Environment:** as for `fix-purpose-question-refresh` (JDK 17, local SDK via `ANDROID_HOME`,
  build-tools 37.0.0, Robolectric 4.14.1 with SDK 34, MockWebServer 4.12.0).
- No device was used and nothing was installed. No production system was contacted; all backend
  responses are fictional fixtures.

## New tests fail on the 1.8.2 rules

With the fixed files copied aside, the selection was made to return nothing (1.8.2 never re-read
review rows) and review rows were routed back to the old failure handler. Then:

| Class | Tests | Failed on 1.8.2 rules |
| --- | --- | --- |
| `ReviewResultRefreshTest` | 4 | 3 |
| `ReviewRefreshWorkerTest` | 4 | 1 |

Failing: `picksRecentReviewAndFailedRowsOfThisPairingOnly`,
`leavesOwnerLabelledRowsAndRowsWithUnsentRepliesAlone`, `readsAtMostFiftyNewestFirst`, and
`failedReadsLeaveTheReviewRowAsItIs` (the row became `failed`). Passing on both:
`nothingIsReadWithoutCredentials` (negative path) and the three worker tests that force a refresh
(`ignored…`, `recorded…`, `unchanged…`). They show the existing result path applies a newer outcome
correctly once a read happens; 1.8.2 never started that read for these rows. The fixed files were
restored and their SHA-1 checksums matched.

## Unit suite

`./gradlew testDebugUnitTest --rerun-tasks --no-build-cache`: exit 0; JUnit XML timestamps
2026-10-29T01:46:30Z to 01:46:37Z; 181 tests, 0 failures, 0 errors, 0 skipped, 27 classes (base 173,
plus 8 new). An earlier incremental run reported a stale `BuildConfig.VERSION_NAME` of 1.8.2 in
`PurposeAnswersTest`; after `clean` it read 1.8.3, which is why every build here was clean.

`./gradlew compileDebugAndroidTestKotlin`: exit 0. Instrumented tests were not run (no device).

## Build

- `./gradlew clean assembleDebug --no-build-cache`: exit 0.
- `app-debug.apk` SHA-256 `3e31c459a855b93ba9c2307908e3479e797a63c31685238dfbce28777d4d19e8`.
- `aapt2 dump badging`: `versionCode='18'`, `versionName='1.8.3'`.
- `dexdump -f`: `BuildConfig.VERSION_CODE` = 18, `VERSION_NAME` = "1.8.3"; no `1.8.2` string in any
  dex; `ReviewResultRefresh` is present.

## OpenSpec and other checks

- `openspec validate refresh-stale-review-rows --strict`: exit 0.
- `openspec validate --all --strict`: exit 0, 7 passed, 0 failed.
- No other open change modifies the same requirement.
- `git diff --check`: exit 0. Room schema unchanged.

## Not verified

- The ten rows on the owner's phone. The owner must install 1.8.3 and open the app; the 29 September
  row may fall outside the 30-day window (see `design.md`).

## Archive

- `openspec archive refresh-stale-review-rows --yes`: exit 0, no warnings; 2 requirements added and
  1 modified in `mobile-notification-completion`.
- `openspec validate --all --strict` after archiving: exit 0, 6 passed, 0 failed.
