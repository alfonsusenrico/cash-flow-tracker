# Design

## Context

See `proposal.md` for the report. The base is `origin/main` at `d33b39a` (2.3.1, code 23, Room 8).
The sender answer code (`SenderReplyReceiver`, `SenderReplies`, `SenderConfirmationNotifier`,
`SenderQuestionControls`, `CompanionViewModel.confirmSender`) is identical in 2.2.0 (`2ea4b5e`) and
2.3.1, so the 13:25 upgrade did not change answer behaviour. Only the platform's delivery of an old
notification's action could differ.

Facts from the owner's phone and the server (2026-10-31, read-only, supplied by the coordinator):

- The 11:40 event is `needs_confirmation` / `sender_confirmation_required` with an active sender
  question. The server logged no `confirm-sender` from the phone in the following 4 hours, only
  2 result polls.
- The phone row is `needs_confirmation`, `check_attempt = 5`. There is no `sender_replies` row for
  it. All 24 existing replies are `accepted`, and the newest is from 06:24.
- At 11:41 a same-amount myBCA income from a different masked sender was recorded automatically.
- Unknown: whether the owner used the notification or Tanya, and whether before or after the
  upgrade. The device log starts at 13:20.

### Failure fingerprint

- **Expected:** an answered question gets a `sender_replies` row, a `confirm-sender` POST, and then
  a refreshed result.
- **Observed:** no row and no POST. The question stays open on the server and on the phone.
- **Affected case:** a myBCA sender question beside a same-amount transfer recorded a minute later.
  Earlier answers the same morning are unaffected.
- **Frequency:** "sometimes". The first known bad state is 11:40 on 2026-10-31.

### Hypothesis ledger

The experiments run in `SenderAnswerPathTest` (Robolectric SDK 34) with the app's real database,
`NotificationResults`, notifier, manifest receiver, `PendingIntent`s with `RemoteInput` fill-in,
and `CompanionViewModel`. The fixture repeats the owner's shape: a sender question (`818`) and, one
minute later, a same-amount income recorded automatically (`819`). The device checks were not run:
the test phone (Realme) was not attached, and the owner's TECNO is out of bounds.

| ID | Mechanism | Experiment | Result |
| --- | --- | --- | --- |
| H1 | Tanya or Rincian shows an answer as done without storing it | `tanyaNameIsStoredForItsOwnQuestion`; Rincian only links to Tanya (`Jawab sekarang`) | Rejected. The view model stores the reply for the right row. |
| H2a | The inline name or `Catat tanpa nama` action drops a normal reply | `notificationNameIsStoredForItsOwnQuestion`, `notificationUnknownIsStoredForItsOwnQuestion` | Rejected. Both store a reply bound to the right row, event and question. |
| H2b | Blank or missing `RemoteInput`, or a locked device, loses the answer silently | `blankNotificationReplyLeavesTheQuestionAnswerable`, `missingReplyResultsLeaveTheQuestionAnswerable`, `lockedDeviceLeavesTheQuestionAnswerable` | **Confirmed defect.** Nothing is stored (correct), but the question is replaced by an action-less "Konfirmasi pengirim" notification. All three failed on `d33b39a`. |
| H2c | A refused question is never offered again | `laterLookupOffersARefusedQuestionAgain` | **Confirmed.** The confirmation alert receipt is already delivered, so a later lookup posts nothing. Failed on `d33b39a`. |
| H2d | HiOS freezes or kills the process during `goAsync`, or holds the broadcast | Needs the device log from 11:40 | Inconclusive. HiOS `Usf_Hiber` froze this app for about 40 minutes on 2026-09-30, so this is plausible. The diagnostics below now separate "never received" from "received, not finished". |
| H2e | An action posted by 2.2.0 does nothing after `install -r` of 2.3.1 | Device check: post with 2.2.0, install 2.3.1, send the action | Not run (Realme absent). The answer code is the same in both versions. |
| H3 | The recorded neighbour's notification takes the answer, or the answer attaches to the wrong row | `recordedNeighbourLeavesTheQuestionAndItsOwnActions` plus the name test | Rejected. Tags, intents and rows are distinct, and the reply binds to `818`. |
| H4 | The unique index or a pairing mismatch swallows the reply | Code reading against the facts | Rejected by observation. There is no earlier row to conflict with, the polls succeed under the row's pairing, and a mismatch shows a visible message. |
| H5 | `819` replaces or cancels `818`'s notification | `recordedNeighbourLeavesTheQuestionAndItsOwnActions` | Rejected. |

### First violated contract

`mobile-sender-replies` → "Invalid reply input stays correctable" is violated in the notification
path, which `SenderReplyReceiver` owns. Every refused or failed attempt (blank or missing input,
locked, timeout, exception, changed question, unpaired) ends in `SenderConfirmationNotifier.showState`.
That replaces the question in place with a notification that has the same title as the "Balasan
tersimpan" state and no actions. The confirmation receipt is already marked delivered, so no later
lookup re-posts the question, and nothing records that an attempt happened.

Combined with any trigger that refuses an attempt, this produces exactly the observed state: no
reply row, no POST, an open question, and an owner who reasonably believes the notification
accepted the answer. Which trigger hit the 11:40 attempt is not proved. H2d and H2e stay open, and
the diagnostics are designed to settle them on the next occurrence.

## Goals / Non-Goals

**Goals:**
- A refused or failed notification answer stays answerable where it was given.
- Every attempt leaves a local, private trace that names where it stopped.
- Tanya tells the truth about an unsaved notification reply.

**Non-goals:** see `proposal.md`. Lookups stay silent, and the backend is unchanged.

## Decisions

### D1. A refusal re-posts the question with its actions

`SenderReplies.offerAgain` reloads the row. If the sender question with the same event and
question id is still open, and no stored answer other than `invalid` exists, it calls
`SenderConfirmationNotifier.post(row, item, problem)`. That builds the same notification in the
same slot, with both actions, and with the reason as the content line and the first sentence of the
expanded text. `setOnlyAlertOnce` keeps it silent. Otherwise, for a stored answer or a closed
question, it shows the message alone, as before.

- Rejected: re-posting from `NotificationResults` on every lookup. That breaks "Question lookups do
  not repeat alerts" and still leaves a dead notification until the next lookup.
- Rejected: clearing the alert receipt on refusal. The next lookup would re-alert, with sound,
  hours later.

### D2. Reason codes in a bounded Room table

`answer_attempts` (Room 9) has the columns `id`, `created_at`, `channel`, `answer_kind`, `row_id`,
`event_id`, `question_id`, `outcome` and `app_version`. `AnswerAttempts.record` inserts a row and
keeps the newest 200 in one transaction. The receiver writes `received` first, then a refusal code
(`malformed`, `locked`, `missing_input`, `timeout`, `error`). `SenderReplies.enqueue` writes the
storage outcome (`queued`, `not_paired`, `invalid_name`, `question_changed`, `already_answered`)
for both channels.

The trace tells the cases apart:
- no row: the broadcast never reached the app;
- `received` alone: the app stopped mid-way (freeze or kill);
- any other code: that refusal.

A failure to write a record is logged as a reason code only and never blocks the answer.

- Rejected: logcat lines. The owner's device log rotates within hours, and it covered only 13:20
  onwards here.
- Rejected: SharedPreferences. Diagnosis already reads a copy of the phone's database, and a table
  can be queried by event.
- No foreign key to `raw_notifications`, so the trace outlives a pruned or deleted row.

### D3. Tanya reflects the newest settled attempt

`CompanionViewModel.answerAttempts` streams the newest 100 settled steps, skipping `received`
steps. For the shown sender question, Tanya takes the newest step. If that step came from the
notification and was not `queued`, the card shows "Balasan dari notifikasi belum tersimpan. Jawab
di sini." (or the locked variant) above the name field. A newer attempt from either channel
replaces it. A stored answer already hides the field and shows its own state.

- Rejected: showing `received`-only attempts as failed. An attempt still in progress would briefly
  look failed.

### D4. Missing `RemoteInput` results are their own code

A name action without `RemoteInput` results is recorded as `missing_input`, separately from
`invalid_name`. A platform that strips the fill-in (H2) then shows up in the trace.

## Data, API and failure behaviour

- No API change. The receiver still validates the action, scheme, UUID segments and `row_id` before
  it touches an answer. A malformed intent records `malformed` without identifiers and changes no
  notification.
- Privacy: the table has no column for a name, text or amount (`SenderAnswerPathTest` asserts that
  the record never contains the typed name). The database stays excluded from cloud backup and
  device transfer (`backup_rules.xml`, `data_extraction_rules.xml`).
- Timing: two small inserts are added inside the existing `goAsync` window, and the 8-second
  storage timeout is unchanged.

## Risks / Trade-offs

- [The fix does not stop HiOS from freezing the app] → The notification stays answerable after a
  refusal, Tanya says so, and the trace shows a freeze as `received` with nothing after it.
- [A `received` step is written but the process dies before it stores or refuses the answer] → The
  app never replaces the question notification in that case. What Android shows for the unanswered
  inline reply is platform behaviour and was not verified on a device. Tanya still lists the
  question as open, and the trace holds `received` alone.
- [Room 9 migration] → It adds one table and one index. `AnswerAttemptMigrationTest` migrates a
  populated version 8 database and checks that every existing definition and row is unchanged.

## Migration Plan

Install 2.3.2 over 2.3.1. `MIGRATION_8_9` creates `answer_attempts` and its index. Rollback means
reinstalling 2.3.1, which needs an uninstall because Room cannot downgrade 9 → 8. Keep the 2.3.1
APK, and prefer a forward fix. No backend step is needed.

## Open decisions for owner

- Resolved on 2026-10-31 by `keep-notification-answers-answerable` (same branch, same 2.3.2). The
  purpose prompt and the account proposal had the same defect and now share the fix. The
  source-pocket notification has no answer path of its own and was unaffected.
- The HiOS per-app power setting for the companion (`Usf_Hiber`) is still the owner's remedy for
  frozen captures and answers.
- Device evidence for H2d and H2e is outstanding. When the Realme is available, run the
  `install -r` experiment. After the next lost answer, read `answer_attempts` for that event from a
  database copy.

## Follow-up (2026-10-31)

`keep-notification-answers-answerable` moved the receiver steps described in D1 and D2 into a
shared `NotificationAnswer` runner, used by the sender, purpose and account receivers.
`SenderReplies.enqueue` now only returns its reason code. The runner records it for the
notification channel and `CompanionViewModel` records it for the app channel. `answer_attempts`
rows carry the answer kind, and `SenderReplies.offerAgain` re-offers the row's current open sender
question, so an action for an earlier question also leaves the current one answerable. Its design
holds the sibling-path experiments.
