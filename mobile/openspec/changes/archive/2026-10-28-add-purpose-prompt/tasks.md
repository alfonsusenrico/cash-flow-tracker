## 1. Contract and storage

- [x] 1.1 Add `PurposeQuestion`, `PurposeSuggestion`, `PurposeAnswerRequest`, the `review_status` and `question` result fields, and `answerPurpose` (D1).
  - Evidence: `PurposeQuestionsTest.backendQuestionParsesWithItsSuggestions` and `answerBodyCarriesExactlyOneAnswer`.
- [x] 1.2 Add `PurposeQuestions.inspect` with the reason codes, the text normaliser and the amount notation (D1).
  - Evidence: `PurposeQuestionsTest` covers valid, absent, text-only and suggestion-only questions, every malformed variant with its code, the text rule and the notation.
- [x] 1.3 Add Room 7 (D2): `MIGRATION_6_7`, the exported `7.json`, the entity fields, the needs-answer query, the prune guards and the reply DAO updates.
  - Evidence: the Robolectric `PurposeMigrationTest` migrates a populated version 6 database built from `6.json`, and Room validates it against version 7. Existing rows and sender replies are kept, and a purpose reply removes the row from "Perlu dijawab". Dropping one `ALTER` makes the test fail.
- [x] 1.4 Persist the question in `NotificationResults.consume`: store the question ID, strip malformed questions, and log only the reason code.
  - Evidence: `PurposeAnswersTest.recordedQuestionIsStoredAndAlertsAsThePrompt` and `malformedQuestionIsDroppedWithItsReasonAndAlertsAsARecord`.

Expected Result: valid questions are stored per row, and malformed ones are dropped with a code, without breaking the record.

## 2. Notification

- [x] 2.1 Add `PurposeQuestionNotifier` (D3, D4): the content builder, the label truncation, the "Apa ini?" channel, the record-slot tag, `showState` and `cancel`.
  - Evidence: `PurposeQuestionNotifierTest` covers the title, the action order and count with and without text, the hidden-suggestion note, truncation including emoji, the posted actions with the `RemoteInput`, the channel importance, the public version, denied permission and refresh-only behaviour. Removing the "Tulis" slot reservation fails two tests.
- [x] 2.2 Route record alerts through `alert()` in `NotificationResults`. Add `PurposeAnswerReceiver` and register it in the manifest.
  - Evidence: `PurposeAnswersTest.refreshOfAnAnsweredQuestionLeavesTheAnswerStateShown`, which fails when the pending-answer rule is removed; the replacement after success in `successSendsTheVersionAndConsumesTheAnsweredResult`; the receiver compiles.

Expected Result: one notification per question, which is replaced once the question closes.

## 3. Answer outbox

- [x] 3.1 Add `PurposeAnswers.enqueue` (D5): stored before the network, reuse of a pending answer, refusal of a different answer, replacement of a failed answer, and validation.
  - Evidence: `PurposeAnswersTest.answerIsStoredBeforeAnyNetworkCallAndDeliveredOnce` and `invalidAnswersAreRefusedBeforeTheyAreStored`.
- [x] 3.2 Add `PurposeAnswers.deliver` and delegate `purpose` replies from `SenderReplyWorker` (D5).
  - Evidence: `PurposeAnswersTest`:
    - success with the 1.8.0 header and the request body;
    - network loss and 503 with backoff, replayed with the same reply ID;
    - 409 `stale_question` and `purpose_answer_conflict` closed with a refresh;
    - 422 and 404 failed, then answered again with a new ID;
    - 401 waiting.
    - Mapping 409 to a retry fails the test.

Expected Result: every server outcome has a tested state, and no answer is lost or applied twice.

## 4. Interface

- [x] 4.1 Add the inbox "Perlu dijawab" filter with its count, the row badge and the cancel on delete (D6).
  - Evidence: the filter query is covered by `PurposeAnswersTest` and `PurposeMigrationTest`; the screen compiles. The on-device check waits for the owner's install.
- [x] 4.2 Add `PurposeQuestionControls` and `PurposeClosedNote`, and wire them first in `TransactionEditSheet` (D6).
  - Evidence: `PurposeQuestionUiTest` (Compose on Robolectric) checks:
    - the heading;
    - the chip descriptions and 48 dp heights at font scale 2;
    - the suggestion and normalised text callbacks;
    - the text rule;
    - the pending, failed and disconnected states.

Expected Result: the owner can answer from the sheet with TalkBack and large text.

## 5. Build and delivery

- [x] 5.1 Bump to 1.8.0 (code 15) (D7). Run `./gradlew clean testDebugUnitTest --no-build-cache --rerun-tasks` and `assembleDebugAndroidTest`, then build with `./gradlew clean assembleDebug --no-build-cache` and dexdump-check `"1.8.0"`.
  - Evidence: the test XML counts and timestamps, and the dexdump output, in `verification.md`.
- [x] 5.2 Validate with `openspec validate add-purpose-prompt`, `--strict` and `openspec validate --all`.
  - Evidence: plain and `--all` exit 0. `--strict` exits 1 only on five "requirement text is very long" warnings, which are recorded as a follow-up in `verification.md`.

Expected Result: a verified 1.8.0 debug build, ready for the owner's review. No device install.

## Workflow follow-up

- Write `verification.md`, archive with `openspec archive add-purpose-prompt --yes`, re-validate, commit with subject-only messages, and push `feat/purpose-prompt`.
- Release order: deploy backend phase 3 (merged as cash-flow-tracker `d12be2a`) first, then install 1.8.0 on the owner's instruction.
