# Proposal

## Approval

Owner authorized phases 2–8 on 2026-10-28 without per-phase review; coordinator-reviewed.

This change fixes a defect the owner reported on 2026-10-29. The coordinator confirmed the root
cause before this change was written, and on the same day added rows awaiting confirmation to its
scope (same version).

## Why

The owner answered "Apa ini?" questions on the tracker's review page (Tinjau). The backend closed
them: the event's `active_question` is empty, the transaction is reviewed, and
`GET /api/ingest/notifications/{event_id}/result` returns no `question`. The phone still lists those
rows under "Perlu dijawab" and can keep showing the "Apa ini?" notification.

The cause is in `NotificationResults.consume` (1.8.1): a `recorded` result sets the row's next
check to none. A recorded row with an open question is never checked again, so only an answer
given on the phone (or its 409) refreshes it. The backend also closes questions through a ledger
edit, a bulk review, an accepted history suggestion and automatic movement pairing, and the phone
sees none of them.

Rows awaiting confirmation (`needs_confirmation`: account-mapping, sender and Jago source-pocket
questions) have the same defect: such a result also ends the checks. On 2026-10-29 a Jago
source-pocket question was answered on the server, the event is now recorded, and the phone still
shows the question as pending.

## Expected Outcome

- A question the backend closes elsewhere leaves "Perlu dijawab" and its notification within
  minutes while the phone can run work, and as soon as the owner opens the app otherwise.
- The "Apa ini?" notification is replaced quietly by the recorded alert, or stays gone when the
  owner already dismissed it, exactly as after an answer given on the phone.
- An answer waiting in the phone's outbox for a question the backend has closed is settled as
  closed and never sent.
- A confirmation answered or settled on the server leaves the phone's pending state and its
  notification the same way, and the recorded alert follows as after a phone answer.
- Checks stop seven days after capture, so an ignored question does not poll forever.

## What Changes

- A recorded row whose result carries a valid open purpose question keeps a scheduled result
  check: 2 min, 10 min, 30 min, 2 h and 6 h after each result that shows it open, then every 12 h,
  and none later than 7 days after capture. The schedule is one small set of named constants.
- A refreshed result without the question, with a different question, or with
  `review_status = reviewed` closes the stored question: the row leaves "Perlu dijawab" and the
  notification is updated through the existing quiet record alert.
- An unsent outbox answer to a closed question becomes `closed` with the code `question_closed`;
  the sheet says the question is finished on the server and the answer will not be sent.
- A row awaiting confirmation keeps the same schedule. A refreshed result that is recorded,
  ignored, or a different question stores the new result, cancels the confirmation notification,
  drops cached source-pocket choices, and delivers the recorded alert as the phone-answer path does.
- A failed check of an open question never changes the row's status, error or result: a transient
  failure moves to the next step, any other failure stops the checks.
- Opening the app checks every open question of the current pairing at once (purpose questions and
  confirmations), whatever the auto-sync switch says, because it only reads results of events the
  server already accepted.
- Version 1.8.2 (code 17).

## Non-goals

- Any backend change. The backend already returns the closed state; only the phone missed it.
- Push from the backend to the phone. The companion has no push channel; this change polls.
- New Android permissions or exemptions. Checks use the WorkManager scheduling the completion
  worker already uses.
- Changing how a recorded row without a purpose question is refreshed or how its failures are
  stored (see the follow-up in `design.md`).
- Settling unsent sender or source-pocket replies locally when their confirmation closes. They keep
  the existing path: the server answers 409 and the reply becomes `stale`.
- Installing the build on a device.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `mobile-purpose-questions`: open questions are re-checked until closed or seven days old, and
  closing elsewhere (or a reviewed transaction) settles the row, its notification and any unsent
  answer.
- `mobile-notification-completion`: automatic result checks also cover recorded events with an
  open purpose question and events awaiting confirmation; a confirmation settled elsewhere clears
  on the phone; a failed re-check keeps an open question; opening the app checks open questions.

## Impact

- Code: `NotificationResults`, `NotificationCompletionWorker`, `SyncScheduler`, `PurposeAnswers`,
  `PurposeQuestions`, a new `QuestionRefresh`, `NotificationDao`, `SenderReplyDao`, `MainActivity`,
  and the version in `app/build.gradle.kts`.
- Room stays at version 7: new queries only, no schema change, no migration.
- No API contract change. No backend change is needed; the owner must install 1.8.2 for the fix
  to take effect.
