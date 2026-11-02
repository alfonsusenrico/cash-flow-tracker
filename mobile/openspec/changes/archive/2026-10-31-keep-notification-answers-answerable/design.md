# Design

## Context

See `proposal.md`. Base `d33b39a`, branch `fix/sender-answer-persistence`, which already carries
`fix-lost-sender-answers` (Room 9 `answer_attempts`, the sender re-offer and the Tanya notice). The
purpose and account receivers and stores were unchanged from `d33b39a` before this follow-up.

### Experiments (Robolectric SDK 34, the app's database, notifiers and receivers)

`PurposeAnswerPathTest`, `MappingAnswerPathTest` and `SourcePocketAnswerPathTest` compile against
`d33b39a`. They were run on a `git archive` export of that revision, then on the branch.

| Path | Case | `d33b39a` | Branch |
| --- | --- | --- | --- |
| Purpose | suggestion, text and Tanya answers stored | pass | pass |
| Purpose | blank text, missing text, locked device, changed question | **fail** (0 of 3 actions left) | pass |
| Purpose | a later lookup re-offers a refused prompt | pass (the silent re-post restores it) | pass |
| Account | confirmation from the notification and from Tanya applied | pass | pass |
| Account | 503, offline, changed question, later lookup | **fail** (0 of 2 actions left) | pass |
| Source pocket | the action only opens the app; Tanya choice stored; Tanya refusal leaves the notification | pass | pass |

Timeout and storage error cannot be triggered through the real receivers without a test seam. The
shared runner's tests (`NotificationAnswerTest`) cover both, as well as a failing re-offer. On
`d33b39a` the code reading is unambiguous: both paths end in the same action-less state message
(`notice` / `showFailure`).

## Decisions

### D1. One runner for every notification answer

`NotificationAnswer(db, kind, rowId, eventId, questionId, messages).run(locked, missingInput, store,
offerAgain)` takes these steps in order:

1. Records `received`.
2. Refuses a locked device or a missing inline reply.
3. Runs `store` within 8 s, or records `timeout`; an exception records `error`.
4. Records the store's own reason code.
5. Re-offers the question unless the answer was stored.

A failing re-offer is logged with a reason code only and never escapes the receiver coroutine. The
sender receiver uses the same runner.

- Rejected: copying the sender receiver's steps into each receiver. That is three copies of the
  same failure handling.

### D2. Re-offer the row's current open question

`offerAgain` re-posts whatever question the row asks now, not the question id carried by the stale
action. An action for an earlier question therefore still leaves the current one answerable. When
an answer is already stored (other than `invalid` for sender, other than `failed` for purpose), or
the row no longer asks, only the message is shown.

### D3. Callers record outcomes; stores return reason codes

`SenderReplies.enqueue`, `PurposeAnswers.enqueue` and `MappingConfirmation.confirm` return codes:
- sender and purpose: `queued`, `not_paired`, `invalid_name`/`invalid_answer`,
  `question_changed`, `already_answered`;
- account: `confirmed`, `server_refused`, `invalid_result`, `network_unavailable`.

The notification runner records them for the notification channel, and `CompanionViewModel` records
them for the app channel. This keeps the stores' signatures, which many existing tests call, unchanged.

### D4. Tanya notice per question type

`AnswerAttempts.noticeFor(attempts, event, question)` takes the newest settled attempt for the shown
question. If that attempt came from the notification and was not stored, Tanya shows
"Balasan/Jawaban/Konfirmasi dari notifikasi belum tersimpan[ karena ponsel terkunci]. Jawab di
sini." through the shared `UnsavedNotice`. An account proposal without a question id matches
`question_id IS NULL`.

### D5. Source pocket is unchanged

The only action of the Jago source-pocket notification is an activity PendingIntent, so no answer
is ever stored or refused from the notification shade. Tanya reports a refusal through its message
and leaves the notification untouched. There is no defect to fix.

## Risks / Trade-offs

- [The purpose prompt is re-posted after a refusal] → It is silent (`setSilent`) and stays in the
  same slot, so no new alert sounds.
- [Account confirmation still sends from a locked screen] → This is unchanged behaviour. It is
  recorded as an open decision rather than changed silently.

## Migration Plan

None beyond Room 9 from `fix-lost-sender-answers`. Rollback is the same as for that change.

## Open decisions for owner

- Account-proposal confirmations from the notification do not require device authentication.
  Sender and purpose actions do (`setAuthenticationRequired` on Android 12+). Making them consistent
  is a behaviour change for the owner to approve.
