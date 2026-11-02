## Approval

Owner authorized phases 2–8 on 2026-10-28 without per-phase review; coordinator-reviewed.

## Why

Many bank notifications name no merchant. For example, myBCA says "You spent IDR 187,500.00 at Groceries". The backend now records these payments with `review_status = needs_review` and attaches an "Apa ini?" purpose question with ranked suggestions (cash-flow-tracker change `add-ingestion-memory-and-purpose-questions`, decisions D9–D14). The companion ignores the question today. The owner wants it asked for every such payment or income on the phone, within seconds of paying, while the purpose is still remembered.

This is phase 4b of the cash-flow-tracker plan `research/notes/2026-10-28-automation-quality-plan.md`.

## Expected Outcome

- A recorded expense or income whose result carries a valid purpose question shows one actionable notification, "Apa ini? Rp187.500 · BCA Harian". It has suggestion buttons and a "Tulis" free-text reply. Tapping it opens the detail sheet.
- The inbox has a "Perlu dijawab" filter with a count, and a badge on each row that still needs an answer.
- The detail sheet shows every suggestion as a chip, plus a text field. It works with TalkBack and has 48 dp targets.
- Each answer is saved on the phone with its own `reply_id` before any network call. It is delivered in the background with retries, and replays are safe.
- A closed or stale question is shown as closed. A rejected answer is shown as failed so the owner can answer again. The notification is updated or replaced when the question is answered or closed.
- The companion identifies itself as 1.8.0, which enables questions on the backend.

## What Changes

- Parse the `question` and `review_status` fields of recorded results. Validate them, and drop a malformed question with a reason code.
- Room schema 7:
  - add the open question id and the reason code to `raw_notifications`;
  - add the suggestion id and the answer text to the existing `sender_replies` outbox.
  - Include the exported schema and a migration test.
- Add a purpose notification on its own channel. It reuses the record's alert slot, so the payment produces one notification, not two. Add a broadcast receiver for its actions.
- Add the purpose answer outbox (`PurposeAnswers`). The existing `SenderReplyWorker` delivers it to `POST /api/ingest/notifications/{event_id}/answer-purpose` and maps 409 to closed, 404/422 to failed, and network or 5xx errors to a retry with backoff.
- Inbox: add the "Perlu dijawab" filter and badge. Detail sheet: add `PurposeQuestionControls`.
- Version 1.8.0 (code 15). `X-Companion-Version` and `source_version` already come from `BuildConfig.VERSION_NAME`.

## Non-goals

- Backend changes. The contract of cash-flow-tracker phase 3 is used as it stands.
- Questions on results that are not recorded, and on movements or investments. The backend does not attach questions to them.
- Answering from the lock screen without authentication.
- Installing on any device. Backend phase 3 is merged; 1.8.0 is installed only after that backend deploy is live.

## Capabilities

### New Capabilities

- `mobile-purpose-questions`: receiving, showing, answering and delivering "Apa ini?" purpose questions on the companion.

### Modified Capabilities

None. `notification-capture` behaviour is unchanged.

## Impact

- **Code** (under `app/src/main/java/.../listener/`):
  - `data/network/model/ApiModels` and `IngestionApiService`;
  - `data/model` (`RawNotificationEntity`, `SenderReplyEntity`), `data/db` (`AppDatabase`, the DAOs);
  - `data/sync` (new `PurposeQuestions`, `PurposeAnswers`; `NotificationResults`, `SenderReplyWorker`);
  - `service/` (new `PurposeQuestionNotifier`), `receiver/` (new `PurposeAnswerReceiver`), `ui/inbox/*`, `ui/components/StatusBadge`;
  - `AndroidManifest.xml`.
- **Database:** Room 6 → 7, additive nullable columns only.
- **API:** adds the existing backend endpoint `answer-purpose`. Requests are unchanged apart from the version string.
- **Tests:**
  - JVM and Robolectric tests for parsing, notification building, the outbox state machine and the migration;
  - an instrumented Compose test for the sheet controls, compiled only.
- **Dependencies (test only):** `mockwebserver` 4.12.0 (already used by the instrumented tests) for JVM tests.
- **Release:** 1.8.0 (code 15) after backend phase 3 is deployed. Older backends send no question, and the app then behaves like 1.7.0.
