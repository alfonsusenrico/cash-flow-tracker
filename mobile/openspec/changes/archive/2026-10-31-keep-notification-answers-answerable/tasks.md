# Tasks

## 1. Reproduce on the sibling paths

- [x] 1.1 Add `AnswerPathHarness`, `MappingFixtures`, `PurposeAnswerPathTest`, `MappingAnswerPathTest` and `SourcePocketAnswerPathTest`, all compiling against `d33b39a`. Verify: on a `d33b39a` export, 4 purpose and 4 account regression tests fail with "expected:<3>/<2> but was:<0>", while the success paths and all source-pocket tests pass (`verification.md`).

Expected Result: purpose and account notification answers are shown to lose their actions on refusal, and source pocket is shown unaffected.

## 2. Shared fix

- [x] 2.1 Add the `NotificationAnswer` runner and move the sender, purpose and account receivers onto it (D1). Verify: `NotificationAnswerTest` (stored, store refusal, locked and missing, timeout, error, failing re-offer, purpose and account traces, malformed action) and the unchanged `SenderAnswerPathTest` pass.
- [x] 2.2 Add `offerAgain` for purpose and account, with problem lines in their notifiers (D2). Verify: the 8 failing tests from 1.1 pass on the branch.
- [x] 2.3 Return reason codes from the stores, record app attempts in the view model, and add the answer kind to the record (D3). Verify: `SenderAnswerPathTest.tanyaNameIsStoredForItsOwnQuestion` still records app `queued`; `PurposeAnswersTest`, `QuestionRefreshTest` and `MappingConfirmationTest` pass.
- [x] 2.4 Show the notice on Tanya's purpose and account cards through `UnsavedNotice` (D4). Verify: `CompanionScreensUiTest` `purposeQuestionSaysANotificationAnswerWasNotSaved` and `mappingQuestionSaysANotificationConfirmationWasNotSaved`, in both themes.

Expected Result: every notification answer path that can refuse keeps its question answerable, traces the attempt, and Tanya says so.

## 3. Verification

- [x] 3.1 Run the full unit suite, compile the instrumented tests, build with `clean assembleDebug --no-build-cache`, and check the version with dexdump. Verify: `verification.md` (498 tests, 0 failures; 2.3.2/24; production URL).
- [x] 3.2 Validate. Verify: `openspec validate keep-notification-answers-answerable --strict` and `openspec validate --all --strict` exit 0.

Expected Result: a verified 2.3.2 build that covers all notification answer paths. Device checks are not run because the Realme is absent.

## Workflow follow-up

- Archive with `openspec archive keep-notification-answers-answerable --yes`, commit, and push `fix/sender-answer-persistence`.
