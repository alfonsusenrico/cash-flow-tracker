## Context

See `proposal.md` for the motivation. The base is `origin/main` at `dc95894` (1.8.1, code 16,
Room 7). The facts this design relies on:

- `NotificationResults.consume` computes the next check only for `queued` and `processing`
  results. Every other status, including `recorded` with an open purpose question and
  `needs_confirmation`, stores `next_check_at = NULL`, so `NotificationCompletionWorker` never runs
  for that event again.
- Only three paths refresh such a row: an answer given on the phone (`PurposeAnswers`,
  `SenderReplyWorker`, `MappingConfirmation`), a 409 to such an answer, and "Sinkronkan sekarang".
- The backend closes a purpose question in `purpose_questions.py` (answer), `merchant_memory.py`
  (ledger edit), `notification_application.py` and `movement_pairing.py` (pairing), and the review
  router; afterwards `compact_result` omits `question`. It resolves sender and source-pocket
  questions server-side as well, after which the event is `recorded`.
- `consume` already does the right thing with a fresh result: a recorded result without a question
  clears `purpose_question_id`, and an alert already delivered is updated with
  `postAlert(activeOnly = true)`, which replaces a shown prompt in the same slot without sound and
  posts nothing when the owner dismissed it. A result that no longer awaits confirmation calls
  `clearConfirmation`, and the pending-alert delivery posts the recorded alert.
- The completion worker's failure handler (`defer`) marks a row `failed` on any terminal error.
  That is right for a queued event and wrong for a recorded or confirmation-pending row.
- HiOS defers this app's WorkManager jobs in the background (`hios-freezer-blocks-listener`). Jobs
  run when the app is in the foreground, and WorkManager's in-process scheduler runs immediately
  eligible work while the process lives.

## Goals / Non-Goals

**Goals:** re-check open questions (purpose and confirmation) on a bounded schedule; settle the
row, its notification and any unsent purpose answer when the server closed the question; check at
once when the owner opens the app; never let a failed re-check corrupt a recorded or pending row.

**Non-goals:** see `proposal.md`. No backend change, no new permission, no Room migration.

## Decisions

### D1. One small schedule for every open question

`QuestionRefresh` holds the schedule as named constants: `DELAYS_MILLIS` = 2 min, 10 min, 30 min,
2 h, 6 h, then `STEADY_DELAY_MILLIS` = 12 h, and `WINDOW_MILLIS` = 7 days after `captured_at`.
`check_attempt` counts the checks that found the same question open; it indexes the delay. A
result with the same question moves one step; a different question (or a row that just became
open) starts at step 0. A check is scheduled only if it falls inside the window; after that the
row keeps its question and only the foreground refresh checks it.

The same-question test compares a key: `purpose:<question_id>` for a purpose question,
`confirm:<event>[:<question_id>]` (the existing confirmation alert key) for a confirmation.

Total cost: about 7 checks in the first 9 hours, then 2 per day, at most about 17 per question.

Rejected: reusing `nextCheckDelay` (10 s doubling to 15 min). It is for processing, would poll an
unanswered question every 15 minutes for a week, and wakes a frozen app for nothing.
Rejected: no window. An ignored question would poll forever.

### D2. A separate unique work name for follow-ups

Follow-ups run `NotificationCompletionWorker` with `QUESTION_FOLLOW_UP = true` under the unique
name `question_follow_up_<event>`. Refreshes keep `notification_completion_<event>` with `KEEP`.

- Outside the worker, `consume` schedules with `REPLACE`, so the latest stored `next_check_at` is
  the one schedule.
- Inside the worker, `consume` gets a no-op scheduler; after storing the check, the worker
  enqueues its successor itself: `APPEND_OR_REPLACE` when it is the follow-up (replacing would
  cancel the running worker), `REPLACE` when it ran as a completion check or refresh.
- A follow-up that runs early (not due) re-enqueues for the remainder; one that finds the question
  closed ends without scheduling.

Rejected: the completion name for follow-ups. With `KEEP`, a pending 12-hour follow-up would
swallow the immediate refresh that a 409 or the foreground check asks for.

### D3. Closing is the existing result path

No new notification code. A follow-up or refresh calls `consume`, which already replaces a shown
"Apa ini?" prompt quietly with the recorded alert or posts nothing when it was dismissed, and
cancels confirmation notifications when a result no longer awaits confirmation. The worker then
delivers pending record alerts, exactly as `SenderReplyWorker` and `MappingConfirmation` do after
a phone answer. In addition, when a row leaves a confirmation, `consume` drops its cached
source-pocket choices (the same thing the 404/409 reply path does).

### D4. `review_status = reviewed` means no open question

`PurposeQuestions.inspect` returns `Absent` for a reviewed result, and `consume` stores every
result without a question it does not keep open. The backend only sets `reviewed` through an owner
action, which also clears the question; the rule guards a result that would still carry one.

Rejected: trusting the question regardless. The owner already classified the transaction, and a
second answer would only add a contradicting label.

### D5. Unsent purpose answers to a closed question become `closed`

Inside the same transaction that stores a valid recorded result, `closeUnsentPurpose` sets every
`queued`, `waiting_auth` or `waiting_pairing` purpose answer of the row whose question is not the
row's current open question to `closed` with `question_closed`. `PurposeAnswers.deliver` sends
only `queued` or `waiting_auth` answers, so nothing is posted. The answer whose own delivery
returned the result is excluded (`answering`) and becomes `accepted` as before.

An earlier attempt of such an answer may have reached the server before its acknowledgement was
lost. So the sheet says "Pertanyaan sudah selesai di server; jawaban ini tidak dikirim lagi." for
`question_closed`, which is true either way, instead of the 409 note that the answer was not used.

Rejected: letting the outbox replay (the backend would answer 409 or replay the stored answer).
The coordinator required no second POST, and it costs a request for a known outcome.

Sender and source-pocket replies are left to their existing path (409 → `stale`), because their
states and notes differ and the coordinator asked only that the question state and notification
clear.

### D6. Failed re-checks never rewrite an open row

For a row with an open question, the worker uses `deferFollowUp` instead of `defer`: it only
updates `next_check_at` and `check_attempt`, guarded by the status and stored result it read.
Network errors, 429 and 5xx move one step; 401, 403, 404 and invalid results stop the checks
(logged with the reason code only). This applies to refreshes too, so opening the app with
rejected credentials no longer turns a recorded row or a pending confirmation into `failed`.

### D7. The foreground check is a result check, not a send

`MainActivity.onResume` calls `QuestionRefresh.refreshOpen`, which enqueues an immediate refresh
for every row of the current pairing with an open question (at most 50, newest first). It runs
whatever "Auto-Sync Notifikasi" says. The switch governs sending captures; `AutoSync` already
documents that result checks for events the server accepted run either way, and this check sends
nothing. It needs a server address and API key. Repeated resumes collapse through the refresh
work's `KEEP` policy.

Rejected: `DirectSync`. It submits pending captures first, which the switch must govern.

## Data, API and failure behaviour

- Room stays at version 7. New queries: `getOpenQuestions`, `updateQuestionCheck`,
  `closeUnsentPurpose`. Existing rows need no backfill: an open row has `next_check_at = NULL` and
  is checked the first time the app opens, which schedules its follow-ups.
- API: only `GET /api/ingest/notifications/{event_id}/result`, already used. No contract change.
- Logs: reason codes and counts only (`question_closed`, check stop reasons).

## Migration and rollback

Install 1.8.2 over 1.8.1 (code 17, same signing key, same application ID). Rolling back to 1.8.1
is an ordinary reinstall; the new work requests are then ignored by the old worker, which only
looks at queued and processing rows. No backend change and no release-order constraint.

## Risks / Trade-offs

- HiOS can freeze the app in the background, so follow-ups may run late; the foreground check
  covers that the moment the owner opens the app.
- A `REPLACE` from outside the worker can cancel a follow-up mid-request; the replacement repeats
  the check.

## Open decisions for owner

- The `question_closed` note wording (D5).

## Follow-ups

- A refresh of a recorded row without an open question still goes through `defer`, so a terminal
  failure (401, 404) marks it `failed`. It predates this change; the same guard could cover every
  recorded row.
- `openspec validate --all --strict` warnings, if any, are listed in `verification.md`.
