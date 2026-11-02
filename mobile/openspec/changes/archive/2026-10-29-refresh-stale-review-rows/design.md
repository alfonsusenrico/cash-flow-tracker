## Context

See `proposal.md`. The base is commit `efea886` on `fix/purpose-question-refresh` (1.8.2, code 17,
Room 7), which added `QuestionRefresh` and the foreground check of open questions. Facts used here:

- `NotificationResults.consume` already applies any refreshed result: `ignored` stores status
  `ignored` with no result and cancels confirmation notifications; `recorded` stores the result,
  inserts the record's alert receipt, and the worker's pending-alert delivery posts the alert once
  per record key; an unchanged `needs_review` result rewrites the same status and error code.
- `getAllNotificationsFlow` hides `ignored` rows; they appear only under the ignored filter.
- On a refresh, the completion worker's `defer` turned any terminal failure (401, 403, 404, invalid
  result) into `failed` with the failure's code, and a transient one into a new error code and a
  scheduled check.
- `labelled_at` is written only by `OwnerLabels` (the owner's edit or send form). Unsent replies are
  `sender_replies` rows in `queued`, `waiting_auth` or `waiting_pairing`.

## Decisions

### D1. Re-read on open only

`ReviewResultRefresh.refresh` runs from `MainActivity.onResume`, after the open-question check. It
enqueues the existing immediate refresh (`notification_completion_<event>`, `KEEP`) for each
selected row. Selection is one query: `needs_review` or `failed`, synced, current pairing, backend
event present, `captured_at` within `WINDOW_MILLIS` (30 days), `labelled_at IS NULL`, no unsent
reply, newest `post_time` first, `LIMIT` 50.

Like the open-question check it ignores the auto-sync switch: it sends no capture and only reads
results of events the server accepted.

Rejected: putting these rows on the `QuestionRefresh` schedule. Their outcome changes rarely and
only on the server's own schedule; up to 17 background checks per row for a week would cost more
than they find, and the next app open catches the change.

### D2. Failed re-reads of review rows change nothing

In the worker, a row in `needs_review` or `failed` that is not an open question gets a no-op failure
handler that logs only the reason code. Status, error code, next check and attempt count stay. This
applies to every refresh of such a row, including "Sinkronkan sekarang", which had the same
`failed` marking.

Rejected: keeping `defer` for `failed` rows. Rewriting a `failed` row's original reason with
`auth_rejected` hides why it failed.

### D3. Owner edits and unsent replies are excluded at selection

The coordinator required that the check never alter a row the owner edited locally or with an
unsent outbox item. Selection skips them. A row the owner labels after it was selected, before the
worker runs, is a narrow race that the existing "Sinkronkan sekarang" path already has; it is
accepted.

### D4. Alerts follow the existing rules

A row that becomes `recorded` gets the usual record alert once per record key, as after any other
completion. It is not silenced: the owner learns the transaction is now in the ledger.

## Data, API and failure behaviour

- Room stays at version 7; one new read query.
- API: only `GET /api/ingest/notifications/{event_id}/result`.
- Logs: reason codes only.

## Migration and rollback

Install 1.8.3 over 1.8.2. Rolling back is an ordinary reinstall; nothing persisted changes shape.

## Open decisions for owner

- The 29 September ShopeePay row sits at the edge of the 30-day window on 29 October and falls
  outside it from then on, so opening the app may not re-read it; "Sinkronkan sekarang" will. Widen
  the window if older review rows should refresh on open too.

## Follow-ups

- None beyond the 1.8.2 follow-up about recorded rows without a question.
