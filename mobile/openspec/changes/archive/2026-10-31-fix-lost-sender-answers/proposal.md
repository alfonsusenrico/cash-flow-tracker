# Proposal

## Approval

The coordinator directed this defect fix on 2026-10-31 for the owner's report that an answered
sender question stayed open on the tracker's Tinjau page. It follows the owner's standing rule for
companion defects: root cause first, no speculative fix, diagnostics when the trigger cannot be
reproduced. No separate owner review of this change's artifacts is recorded; the owner reviews
the pushed branch before merging.

## Why

On 2026-10-31 the owner believed they had confirmed the sender of a myBCA incoming transfer
(11:40). The server never received a `confirm-sender` request. The phone's database holds no
`sender_replies` row for that question, and its notification shows no pending reply. The owner
reports this happens "sometimes".

The investigation (see `design.md`, hypothesis ledger) reproduced the normal answer paths
(notification name, `Catat tanpa nama`, Tanya) and found that all of them store the answer
correctly, even beside a same-amount transfer recorded a minute later. It did find one violated
contract. When the companion refuses or fails to store an answer sent from the notification (blank
or missing input, a locked device, a timeout, an error, or a changed question), it replaces the
question with an action-less "Konfirmasi pengirim" notification, styled like the success state. It
never offers the question there again, because the question's alert receipt is already delivered,
and it leaves no trace of the attempt. One refused attempt therefore looks finished and leaves the
question open. Which refusal (or a platform drop such as the HiOS freezer) hit the 11:40 question
cannot be proved without a device log from that time.

## Expected Outcome

- An answer from the notification that is not stored leaves the question answerable in the
  notification, with a line saying the reply was not saved, and nothing is queued.
- Every answer attempt leaves a local, privacy-safe record of reason codes. The next lost answer
  then shows whether the attempt never reached the app, stopped mid-way, or was refused, and why.
- Tanya says when the newest notification reply for the shown question was not stored.

## What Changes

- The sender reply receiver re-posts the question with its two actions and a reason line after any
  refused or failed attempt, instead of an action-less state notification. A question that already
  has a stored answer, or that is no longer open, still shows only the message.
- A new local table `answer_attempts` (Room 9) records each step of a sender answer: `received`,
  then `queued` or a refusal code, with channel (`notification` or `app`), row and server ids, and
  the app version. It never stores a name, typed text or an amount. It keeps the newest 200 rows and
  is excluded from backup with the rest of the database.
- Tanya's sender card shows "Balasan dari notifikasi belum tersimpan. Jawab di sini." (or the
  locked variant) while the newest settled attempt for that question is a refused notification
  reply.
- The lock-screen message becomes "Buka kunci lalu balas lagi.", because the notification now stays
  answerable.
- Version 2.3.2 (code 24).

## Non-goals

- Any backend change. The backend's sender question and `confirm-sender` contract are unchanged.
- A cure for OEM process freezing (HiOS `Usf_Hiber`). That remains an owner-side setting. This
  change only makes such a loss visible and diagnosable.
- Re-alerting a question on every lookup. Lookups stay silent, and the question stays answerable
  because the refusal itself keeps the actions.
- Source-pocket, mapping and purpose answers. Their receivers are not changed here (see
  "Open decisions for owner" in `design.md`).

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `mobile-sender-replies`: a refused or failed reply from the notification stays correctable
  there, and each answer attempt leaves a private reason-code record that Tanya reflects.

## Impact

- Code: `SenderReplyReceiver`, `SenderReplies`, `SenderConfirmationNotifier`, a new
  `AnswerAttempts`, `AnswerAttemptEntity` and `AnswerAttemptDao`, `AppDatabase` (version 9 and
  `MIGRATION_8_9`), `CompanionViewModel`, `QuestionsScreen`, `SenderQuestionControls`, and the
  version in `app/build.gradle.kts`.
- Room 8 → 9 adds one table and one index. Existing tables and rows are untouched.
- No API change. The owner installs 2.3.2 for the fix and the diagnostics to take effect.
