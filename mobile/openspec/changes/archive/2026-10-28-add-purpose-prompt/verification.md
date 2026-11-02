# Verification: add-purpose-prompt

Date: 2026-10-28

## Revision

- **Base:** `51f0645b71e5a3b05672b643237a6af71fefcf74` (`origin/main`, 1.7.0, Room 6).
- **Branch:** `feat/purpose-prompt`. The results below were produced on the uncommitted working tree that the feature commit records.
- **Environment:**
  - `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`, with `ANDROID_HOME` set to the local SDK;
  - Android build-tools 37.0.0 for `aapt2` and `dexdump`;
  - OpenSpec CLI 1.14.1;
  - Robolectric 4.14.1 with SDK 34.
- **Backend contract:** the archived cash-flow-tracker change `2026-10-28-add-ingestion-memory-and-purpose-questions` (D9–D14), with `purpose_questions.py` and `routers/ingest.py`, read before implementation. The backend was not changed.

## Unit tests

Command: `./gradlew clean testDebugUnitTest --no-build-cache --rerun-tasks`, exit status 0.

- **Totals across the result XML:** 138 tests, 0 skipped, 0 failures, 0 errors.
- **Timestamps:** `2026-10-28T07:55:04` to `07:55:09` (UTC).
- **Baseline on `51f0645`:** 109 tests, all passing.

| Suite | Tests | Covers |
| --- | --- | --- |
| `PurposeQuestionsTest` | 8 | Parsing the backend-shaped question. Absent questions. Text-only and suggestion-only questions. Every malformed variant with its reason code (`unsupported`, `mismatch`, `invalid`). Broken JSON. The text rule (NFKC, trim, limit, control, format and private-use characters). Rupiah notation. Account label. Answer body keys. |
| `PurposeQuestionNotifierTest` (Robolectric) | 6 | The title "Apa ini? Rp187.500 · BCA Harian". At most 3 actions, with "Tulis" last when text is allowed. Three suggestions when it is not. The "n pilihan lain" note. Word-boundary and emoji truncation. The posted notification: channel, record-slot tag, action labels, `RemoteInput` only on "Tulis", `IMPORTANCE_HIGH`, public version, content intent. `showState` without actions. Denied `POST_NOTIFICATIONS`. Refresh-only when not shown. |
| `PurposeAnswersTest` (Robolectric, in-memory Room, real `ApiClient`, `MockWebServer`) | 10 | The question is stored and alerts as the prompt. A malformed question is stripped with its code and alerts as a record. An answer is stored before any request, and a repeated answer is reused while a different one is refused. Success: request path, header `X-Companion-Version: 1.8.0`, body keys, accepted state, text cleared, question cleared, result description updated, recorded alert replaces the prompt. Network drop, then 503, then success: backoff 30 s and 60 s, the same `reply_id` on all three requests, no request after acceptance. 409 `stale_question` and `purpose_answer_conflict`: closed, refresh, note. 422 `unknown_suggestion` and a string-detail 404: failed, re-answer with a new reply ID. 401: waiting, no retry. Invalid answers are refused before storage. A refresh after an answer leaves the notification alone. |
| `PurposeMigrationTest` (Robolectric) | 1 | A populated version 6 database (from `6.json`) migrates to 7 and passes Room's validation. Rows and a sender reply keep their values, with null new columns. The reply stays recoverable. The needs-answer query works. A purpose reply with text can be stored. |
| `PurposeQuestionUiTest` (Compose on Robolectric) | 4 | At font scale 2: the heading, the chip descriptions "Jawab: …, kategori …", 48 dp heights, click actions, the suggestion and normalised text callbacks, the text rule on empty input. The pending state hides the controls. The failed state shows its reason and the controls. Disconnected disables. |
| Existing suites | 109 | Unchanged and passing. |

During development, the first targeted run failed `successSendsTheVersionAndConsumesTheAnsweredResult` with `expected:<1.[8].0> but was:<1.[7].0>`. The cause was a stale `BuildConfig` from the incremental build. A clean `--no-build-cache` run passed, which is why the release build below uses the same flags.

### Mutation checks (the tests catch the faults they guard)

The production code was restored from a copy after each check. The final run above passed afterwards.

1. **Migration column.** Removing `ALTER TABLE sender_replies ADD COLUMN answer_text` from `MIGRATION_6_7` failed `PurposeMigrationTest` (1 of 1).
2. **409 mapping.** Making the 409 branch unreachable, so the answer was retried instead, failed `PurposeAnswersTest` (1 of 10).
3. **"Tulis" slot.** Taking `MAX_ACTIONS` suggestions even when text is allowed failed 2 of 6 in `PurposeQuestionNotifierTest`.
4. **Pending-answer rule.** Removing the `activeOnly -> true` branch from `NotificationResults.alert` failed `PurposeAnswersTest.refreshOfAnAnsweredQuestionLeavesTheAnswerStateShown`.

## Instrumented tests

`./gradlew assembleDebugAndroidTest` exited 0, so all existing instrumented tests compile against the new signatures. They were not run.

A device was attached, and this phase forbids installing on any device. The Compose UI checks for the new controls run on Robolectric instead (`PurposeQuestionUiTest` above). They are not duplicated as instrumented tests.

## APK

Command: `./gradlew clean assembleDebug --no-build-cache`, exit status 0.

- **APK:** `app/build/outputs/apk/debug/app-debug.apk`, SHA-256 `9ce7622e592346dde76385afec1bc5c7d523aac81eb40ac4f27043ea7122261f`.
- **`aapt2 dump badging`:** `name='com.alfonsusenrico.financialtracker.listener.debug' versionCode='15' versionName='1.8.0'`.
- **`dexdump`:**
  - `BuildConfig.VERSION_NAME = "1.8.0"` and `VERSION_CODE = 15` (`classes5.dex`);
  - the inlined `const-string "1.8.0"` in `ApiClient` (the `X-Companion-Version` header) and in `NotificationProcessor` (`source_version`), in `classes11.dex`;
  - no `"1.7.0"` string in any dex.
- The APK was not installed.

## OpenSpec

- `openspec validate add-purpose-prompt`: valid, exit 0.
- `openspec validate --all`: exit 0.
- `openspec validate add-purpose-prompt --strict`: exit 1, on warnings only. Five requirements have text over 500 characters. They are recorded as a follow-up, and the specs were not reshaped only to silence them.

## Not verified

- **On-device behaviour.** Not checked: the heads-up, the shade actions with authentication, `RemoteInput` on HiOS, TalkBack on the real sheet, and the live backend round trip. They need the owner's install, after the backend phase 3 deploy.
- **The live backend.** Not exercised. The contract was followed from the source, and the tests use a fictional server with the documented shapes.

## Follow-ups

- Split the five long requirements into shorter ones when this capability is next touched (`--strict` warnings).
- After the install, check on the device: the heads-up prompt, the two suggestion actions and "Tulis", the answer from the lock screen (an authentication prompt), and the prompt turning into the recorded alert.
