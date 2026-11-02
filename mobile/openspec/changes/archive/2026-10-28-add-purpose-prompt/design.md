## Context

See `proposal.md` for the motivation. The backend contract is the archived cash-flow-tracker change `2026-10-28-add-ingestion-memory-and-purpose-questions`. Its decisions D11–D13 define the result fields, the answer endpoint and the version gate. The backend code is `backend/app/services/purpose_questions.py` and `backend/app/routers/ingest.py` on cash-flow-tracker `origin/main`.

The backend facts this design relies on:

- `compact_result` adds `review_status` and an open `question` to recorded expense or income results. It does so only when the event's `source_version` is at least 1.8.0. The companion sends `source_version` and `X-Companion-Version` from `BuildConfig.VERSION_NAME`.
- Errors come back as `{"ok": false, "detail": {"code": …}}`, except for the 404 of an unknown event, whose `detail` is a string.
- A replay with the same `reply_id` and the same answer returns 200 with the current result. A different answer under that `reply_id` returns 409 `purpose_answer_conflict`.
- After an answer, the result has the new description and no question.

On the companion, 1.7.0 already has the owner-answer pattern:

- a durable `sender_replies` outbox with an `answer_kind` (`sender`, `source_pocket`);
- `SenderReplyWorker` with bounded backoff (`senderRetryDelay`);
- `SenderReplies.recover()` at app start;
- actionable notifications with `RemoteInput` and authentication-required actions;
- in-app controls in `TransactionEditSheet`.

Recorded results alert through `TransactionNotifier`, with the record key as the notification tag and once-only alert receipts.

## Goals / Non-Goals

**Goals:**

- The owner can answer within seconds, from the notification shade or the app.
- No answer is lost or applied twice.
- Malformed or unexpected data never breaks the record pipeline.

**Non-goals:**

- Re-ranking or editing suggestions on the phone.
- Showing the backend's `answer` summary beyond the refreshed result.
- Questions on unrecorded rows.

## Decisions

### D1. Validate at the boundary and keep only what the phone uses

`PurposeQuestions.inspect` returns `Absent`, `Valid` or `Malformed(reason)`.

**Rules:**

- The question must sit on a complete recorded expense or income result with a UUID event and transaction.
- Its `transaction_id`, `direction`, `amount` and IDR `currency` must equal the result's.
- The question ID is a UUID. The prompt is 1–500 characters.
- `allows_text` is present. When it is true, `text_max_length` is 1–200.
- There are at most 5 suggestions, with distinct IDs matching `[A-Za-z0-9_-]{1,16}` and labels of 1–160 characters.
- At least one answer path exists.

**Reason codes:** `purpose_question_unsupported` (another type), `purpose_question_mismatch` (does not belong to this record) and `purpose_question_invalid`.

**On a malformed question:**

- The question is removed from the stored `result_json`.
- The code goes to the row and to a log line that carries only the code.
- The record then behaves as in 1.7.0.

The Gson model keeps only `id`, `kind`, `label` and `category_name` of a suggestion. The phone shows these. The answer is sent by ID, so the merchant, notes and category IDs never need to be stored on the device.

*Rejected:* storing the raw question and validating it on every read. A malformed question would then stay attached to the row, and every reader would need the same defensive code.

*Rejected:* failing the whole result on a malformed question. The money is recorded, and the record must still complete and alert.

### D2. Room 7: two columns on each existing table

- `raw_notifications.purpose_question_id`: the open, valid question, or null.
- `raw_notifications.purpose_question_error`: the D1 reason code.
- `sender_replies.suggestion_id` and `sender_replies.answer_text`: the answer, with `answer_kind = 'purpose'` and `action` set to `suggestion` or `text`.

`MIGRATION_6_7` adds the four nullable columns. The unique index `(pairing_context, event_id, question_id)` already allows one answer per question.

The "Perlu dijawab" query selects rows with `processing_status = 'recorded'`, a question ID, and no purpose reply for that question other than a `failed` one. The two prune queries keep rows with an open question.

*Rejected:* a new `purpose_replies` table. It would duplicate the recovery query, the worker identity and the cascade on row deletion. `answer_kind` already separates two kinds of answer.

*Rejected:* reading the open question from `result_json` with SQL `LIKE`. It would be fragile, and it would not allow an index-free exact filter by question ID.

### D3. The prompt uses the record's alert slot

`PurposeQuestionNotifier` posts with tag = record key and the same ID formula as `TransactionNotifier`.

**Routing.** `NotificationResults` sends a record through `alert()`:

- a valid open question with no answer → the prompt;
- an existing answer on a refresh → no change, so the notification keeps the answer's state;
- a failed answer on first delivery → the prompt again;
- otherwise → the ordinary recorded alert.

**When the question closes.** The next consumed result has no question, so the existing `activeOnly` update of `TransactionNotifier` replaces a prompt that is still shown, silently and in the same slot. This is how the notification is updated after an answer, a 409, or an answer given on the web.

**Channel.** The prompt has its own channel `purpose_questions`, "Apa ini?", at `IMPORTANCE_HIGH`, so it appears as a heads-up. Lock-screen visibility is private, with a generic public version.

*Rejected:* a separate notification next to the recorded alert. One payment would produce two notifications, and the alert receipts would need a second key.

### D4. At most three actions, and "Tulis" keeps its slot

Android's notification templates show at most three actions (`MAX_ACTION_BUTTONS = 3`) and drop the rest without warning. With text allowed, the prompt shows the two highest-ranked suggestions and "Tulis". Without text, it shows three suggestions. The text notes how many more suggestions the app has, and tapping the notification opens the sheet with every suggestion.

**Labels.** Whitespace is collapsed. A label over 24 code points is cut to 23, at the last space when that space is in the second half, and "…" is added.

**Intents.**

- Suggestion actions use immutable broadcasts.
- The text action is mutable, because Android attaches the `RemoteInput` result to it.
- Each intent has a distinct data URI: `cft://purpose/<event>/<question>/<pairing>/<suggestion|text>/<id>`.
- On API 31+, actions require authentication.
- `PurposeAnswerReceiver` refuses to answer on a locked device on API 26–30, and it updates the notification to say so.

*Rejected:* three suggestions plus "Tulis", as first requested. Android would silently drop "Tulis", the only way to answer when no suggestion fits. See Open decision 1.

### D5. Durable outbox in `PurposeAnswers`, delivered by `SenderReplyWorker`

**`enqueue`:**

1. Runs in a Room transaction.
2. Checks that the question is still the row's open question, the pairing and event match, the suggestion belongs to the question, and the text is valid. Text follows the backend rule: NFKC, trimmed, 1–limit code points, and no Unicode "Other" category characters.
3. Reuses a pending reply for the same question, or replaces a `failed` one with a new reply ID.
4. Inserts the reply, then updates the notification to "Jawaban tersimpan; mengirim ke server." and schedules the worker with `KEEP`.

**`deliver`.** `SenderReplyWorker` hands `purpose` replies to `PurposeAnswers.deliver`. This keeps the class identity of persisted work requests and the existing start-up recovery. The outcomes:

| Server outcome | State | Effect |
| --- | --- | --- |
| 200, `ok`, same event and payload hash, result consumed | `accepted` | text cleared; alerts delivered |
| 200 that cannot be consumed | `queued` | retry (`invalid_result`) |
| 409 | `closed` (code from `detail.code`, default `stale_question`) | result refresh scheduled |
| 404 | `failed` (`detail.code` or `event_not_found`) | owner may answer again |
| 400, 422 | `failed` (`detail.code` or `invalid_purpose_answer`) | owner may answer again |
| 401, 403 | `waiting_auth` | recovered after the credentials are fixed |
| 429, 5xx, network error | `queued`, attempt + 1 | `senderRetryDelay` backoff, 30 s doubling to 1 h, `APPEND_OR_REPLACE` |
| pairing changed | `waiting_pairing` | never sent to another server |

**Refresh after success.** The answer endpoint returns the post-answer result, and consuming it is the refresh. It updates `result_json` and the description, clears `purpose_question_id`, and replaces the notification. No extra GET is made.

**Collaborators.** Scheduling, refresh, notification state, results, the API service and the clock are constructor parameters. The JVM tests drive the real state machine with in-memory Room and a fictional server.

*Rejected:* a new worker class. Recovery would need to dispatch by kind, and nothing would be gained.

*Rejected:* treating 409 as `failed`. The backend closes the question, so a new answer could never succeed.

### D6. Inbox and sheet

- **Filter.** `FilterChipType.NEEDS_ANSWER`, "Perlu dijawab", sits second, and its label carries the count.
- **Badge.** Rows have a text badge, "Apa ini? Perlu dijawab".
- **Sheet.** `PurposeQuestionControls` comes first in the sheet: a heading, the prompt, the answer state in a polite live region, suggestion chips in a `FlowRow`, a text field with a counter, and a send button.
  - Chips are at least 48 × 48 dp. They read "Jawab: <label>, kategori <category>" and show the category as a second line.
  - Inputs are hidden while an answer is pending. They return with the reason after a failure.
  - The controls are disabled when the row belongs to another pairing.
  - `PurposeClosedNote` stays visible after a 409.
- **Delete.** Deleting the row from the phone cancels its prompt.

### D7. Version 1.8.0 (code 15)

`ApiClient` already sends `X-Companion-Version: BuildConfig.VERSION_NAME`, and captures store `source_version` from the same constant. Bumping `versionName` is enough. The release APK is built with `--no-build-cache`, and the dexdump check confirms the constant. A stale `BuildConfig` from the build cache made the version test fail during development, which shows the check is needed.

## Data, API and failure behaviour

- **Request:**
  - `POST api/ingest/notifications/{eventId}/answer-purpose`;
  - body `{"question_id", "reply_id", "suggestion_id"}` or `{"question_id", "reply_id", "text"}`, with `null` fields omitted by Gson.
- **Response:** `NotificationResultResponse`. The `answer` field is ignored.
- **Logging:** only reason codes (`purpose_question_*`). No prompt, label, answer text or amount.
- **Retention:**
  - Accepted answers keep the suggestion ID but not the text.
  - Closed and failed answers keep their row out of pruning, as sender answers do, so their note stays visible.

## Risks / Trade-offs

- [Heads-up prompts may feel intrusive] → The owner can lower the "Apa ini?" channel in Android settings. Questions remain in "Perlu dijawab".
- [An answer is stored while the backend already closed the question on the web] → The worker gets 409 and shows the closed note. The refresh removes the question.
- [Events captured by 1.7.0 and submitted after the upgrade carry `source_version` 1.7.0] → They get no question unless retried, which is acceptable for the short upgrade window.
- [A notification action on API 26–30 from the lock screen] → It is refused with an "unlock and answer in the app" note.

## Migration Plan

1. Merge. Backend phase 3 is already merged on cash-flow-tracker `main` (`d12be2a`). Install 1.8.0 only after that backend deploy is live.
2. Room 6 → 7 runs on first start. It only adds columns.
3. **Rollback:** reinstall 1.7.0. Room has no down-migration, so a downgrade requires clearing app data. Older backends never send questions, so 1.8.0 against an old backend behaves like 1.7.0.

## Open decisions for owner

1. **Two suggestion buttons plus "Tulis", not three plus "Tulis"** (D4). Android shows at most three notification actions. Every suggestion is one tap away in the sheet that the notification opens. If the owner prefers three suggestion buttons without "Tulis" in the shade, change `MAX_ACTIONS - 1` in `PurposeQuestionNotifier.content`.
2. **Heads-up importance** for the "Apa ini?" channel, chosen for the "answer within seconds" goal.
3. **The prompt replaces the recorded alert** for the same payment (D3), so the owner sees one notification. After the answer, it becomes the recorded alert with the new description.
