# Verification: show-tags-in-companion

Tested on 2026-11-16, branch `feat/companion-tags` at `9fb9f6f8` (base `abc0199e`, tracker PR #119 merged) plus the uncommitted implementation.

## Checks

| Check | Result |
| --- | --- |
| `./gradlew testDebugUnitTest --rerun-tasks` (JDK 17) | 655 tests, 0 failures, 0 errors; 6 skipped (`CaptureLocalBackendTest`, which needs a running local tracker and is skipped in CI too) |
| `./gradlew lintDebug` | 0 errors; no warning in a changed file |
| `./gradlew assembleDebug` | built `app-debug.apk` |
| `git diff --check` | clean |
| `openspec validate --all --strict` (in `mobile/`) | 11 passed, 0 failed |

## Scenario evidence

| Scenario | Evidence |
| --- | --- |
| A tagged capture | `CaptureFlowUiTest.captureRincianListsTheTagsTheTrackerRecorded` ("Tag" row with "fellowship coach") |
| Two tags | `CaptureFlowUiTest.aRecordedCaptureListsItsTagsOnOneLine` ("Tag: fellowship coach, Malang trip" on the result card) |
| No tags or an older tracker | `CaptureFlowUiTest.aRecordedCaptureWithoutTagsShowsNoTagLine`; `CaptureModelTest.aRecordedCaptureReadsItsTagsAndToleratesTrackersWithout` (empty list, missing field, blank names skipped, moves carry none) |
| A stored capture from before this version | `CaptureFlowUiTest.captureRincianOfAnOlderCaptureHasNoTagRow` |
| Edit a tagged capture | `CaptureModelTest.anEditOnThePhoneNeverSendsTagFields` (the serialized edit request has no `tag_ids`, `new_tags` or `tags`); after an edit, `TextCaptures.edit` refreshes the capture view from the tracker, which carries the current tags |

## Not verified

- **Screenshots.** A throwaway Robolectric capture (`captureToImage`) timed out twice and was removed; the screens are checked by the Compose tests above. The owner sees the result on the phone after installing `android-v2.4.5`.
- **A live capture on a device.** No phone was used; the release APK is checked after the merge (task 3.3).
