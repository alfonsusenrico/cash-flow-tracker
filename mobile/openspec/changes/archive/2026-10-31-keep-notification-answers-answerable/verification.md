# Verification: keep-notification-answers-answerable

Date: 2026-10-31

## Revision

- **Base:** `d33b39a`, through a `git archive` export in the session scratch folder. It was not a
  git worktree.
- **Branch:** `fix/sender-answer-persistence` at `2c915d0`, plus this follow-up's working tree.
- The environment is as in `2026-10-31-fix-lost-sender-answers/verification.md`. No device was used
  (the test phone, a Realme, was absent), and no adb command beyond the earlier `adb devices -l` was run.

## Sibling paths on `d33b39a`

The final versions of `PurposeAnswerPathTest`, `MappingAnswerPathTest` and
`SourcePocketAnswerPathTest` were run on the export:

| Test | `d33b39a` | Branch |
| --- | --- | --- |
| Purpose `suggestionIsStoredForItsOwnQuestion`, `textIsStoredForItsOwnQuestion`, `tanyaAnswerIsStoredForItsOwnQuestion`, `laterLookupOffersARefusedPromptAgain` | pass | pass |
| Purpose `blankTextLeavesThePromptAnswerable`, `missingTextLeavesThePromptAnswerable`, `lockedDeviceLeavesThePromptAnswerable`, `answerToAChangedQuestionLeavesTheOpenPromptAnswerable` | **fail**: expected 3, was 0 | pass |
| Account `confirmationFromTheNotificationIsApplied`, `tanyaConfirmationIsApplied` | pass | pass |
| Account `serverFailureLeavesTheProposalAnswerable`, `offlineConfirmationLeavesTheProposalAnswerable`, `confirmationOfAChangedQuestionLeavesTheOpenProposalAnswerable`, `laterLookupOffersARefusedProposalAgain` | **fail**: expected 2, was 0 | pass |
| Source pocket: all 3 | pass | pass |

## Unit suite (XML, `testDebugUnitTest --rerun-tasks`)

| State | Tests | Failures | Errors | Skipped |
| --- | --- | --- | --- | --- |
| `d33b39a` baseline | 452 | 0 | 0 | 6 |
| After `fix-lost-sender-answers` | 468 | 0 | 0 | 6 |
| After this follow-up | 498 | 0 | 0 | 6 |

The 30 new tests:
- `PurposeAnswerPathTest` (8)
- `MappingAnswerPathTest` (6)
- `SourcePocketAnswerPathTest` (3)
- `NotificationAnswerTest` (9)
- two `CompanionScreensUiTest` cases run in both themes (4)

## Instrumented tests

`./gradlew -PcompanionVerification compileDebugAndroidTestKotlin` succeeded. The sources are
identical to `d33b39a` again. The tests were not run, because no permitted device was attached.

## Build identity

`./gradlew clean assembleDebug --no-build-cache` succeeded.

- `aapt2`: `versionCode='24' versionName='2.3.2'`.
- `dexdump`: "2.3.2" appears in `ApiClient`'s version interceptor, `NotificationProcessor.toEntity`
  and `AnswerAttempts.record`.
- `dexdump`: "https://finance.alfonsusenrico.com" appears in `BuildConfig.<clinit>` and
  `AppPreferences.<init>`.
- The only other 2.3.x string, "2.3.3", belongs to Google's datatransport library.

## OpenSpec

`openspec validate keep-notification-answers-answerable --strict` and `openspec validate --all --strict` exit 0.
