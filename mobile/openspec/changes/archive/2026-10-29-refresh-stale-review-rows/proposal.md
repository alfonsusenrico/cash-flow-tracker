# Proposal

## Approval

Owner authorized phases 2–8 on 2026-10-28 without per-phase review; coordinator-reviewed.

The coordinator requested this change on 2026-10-29, after installing 1.8.2 on the owner's phone,
as a separate change on the unmerged branch `fix/purpose-question-refresh`.

## Why

After 1.8.2 the phone still had 10 rows in `needs_review` (GoPay, Jago and Stockbit noise codes from
10–26 October, and one ShopeePay `model_uncertain` from 29 September) whose backend events are now
`ignored` (9) or `recorded` (1). The backend changed those outcomes after the phone stored them,
through `reclassify_review_noise` at startup and later processing. The phone treats `needs_review`
and `failed` as final and never reads them again, the same kind of defect that 1.8.2 fixed for open
questions.

## Expected Outcome

- Opening the app brings recent review and failed rows in line with the server: noise the server
  now ignores disappears from the list and the review filter, and a settled record shows as
  recorded.
- Nothing the owner changed on the phone, or still has to send, is overwritten.
- A failed read never makes a review row look worse than it was.

## What Changes

- When the app comes to the foreground, re-read once at most 50 rows, newest first, in
  `needs_review` or `failed` with a backend event, the current pairing, captured in the last 30
  days, without an owner label and without an unsent reply. Apply each result through the existing
  result path.
- A failed re-read of such a row (401, 403, 404, 429, 5xx, network) leaves it exactly as it was;
  before, a terminal failure marked it `failed` with a new error code.
- No scheduled follow-ups for these rows: the backend changes them rarely, and the open-app check
  is enough.
- Version 1.8.3 (code 18).

## Non-goals

- Any backend change, or a push from the backend.
- Re-reading rows older than 30 days on open. "Sinkronkan sekarang" still refreshes the newest 50
  review, failed, confirmation and recorded rows, whatever their age.
- Changing rows the owner labelled on the phone or that have an unsent reply.
- Installing the build on a device.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `mobile-notification-completion`: opening the app re-reads recent review and failed results; a
  failed re-read leaves such a row unchanged; the polling rule says review outcomes are re-read on
  open but never polled on a schedule.

## Impact

- Code: a new `ReviewResultRefresh`, `NotificationDao.getRefreshableReviews`,
  `NotificationCompletionWorker` (failure handling for review rows), `MainActivity`, and the version
  in `app/build.gradle.kts`.
- Room stays at version 7: one new query, no schema change.
- No API contract change and no backend change; the owner must install 1.8.3.
