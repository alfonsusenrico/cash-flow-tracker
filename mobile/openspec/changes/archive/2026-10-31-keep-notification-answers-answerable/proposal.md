# Proposal

## Approval

The coordinator directed this extension on 2026-10-31, on the branch of `fix-lost-sender-answers`
(archived as `2026-10-31-fix-lost-sender-answers`), at the same version 2.3.2 (code 24). The owner
reviews the pushed branch before merging.

## Why

`fix-lost-sender-answers` found that a refused sender reply from the notification replaced the
question with an action-less notification. Its open decision asked whether the sibling notification
answer paths fail the same way. The same experiments on `d33b39a` show that:

- The "Apa ini?" prompt turns any refused answer (blank or missing text, locked device, changed
  question) into an action-less "Apa ini?" message. Unlike sender, the next lookup re-offers it.
- The account-proposal notification turns any failed confirmation (server error, offline, changed
  question) into an action-less "Konfirmasi gagal". The next lookup never re-offers it.
- The Jago source-pocket notification is not affected: its only action opens the app, and refusals
  in Tanya are reported in place.

Neither affected path leaves any trace of the attempt.

## Expected Outcome

- A refused or failed answer from the purpose or account notification leaves that question
  answerable in its notification, with a line saying it was not saved.
- Every purpose and account answer attempt leaves the same private reason-code trace as sender
  answers, and Tanya shows the unsaved-answer notice for those question types.

## What Changes

- One shared notification-answer runner (`NotificationAnswer`) now carries the steps every receiver
  takes: trace `received`, refuse a locked phone or a missing inline reply, store within the
  broadcast budget, record the outcome, and re-offer the question. It replaces the copies in the
  sender, purpose and account receivers.
- `PurposeAnswers.offerAgain` and `MappingConfirmation.offerAgain` re-post the row's open question
  with its actions and the reason, and the notifiers take an optional problem line.
- `answer_attempts` rows carry the answer kind (`sender`, `purpose` or `account`). Tanya's purpose
  and account cards show the shared unsaved-answer notice.
- The view model records attempts made in the app for all three kinds. The domain stores now return
  reason codes and no longer record attempts themselves.
- No schema change beyond Room 9 from `fix-lost-sender-answers`. The version stays 2.3.2 (code 24).

## Non-goals

- Source-pocket answers, which have no notification answer path (see `design.md`).
- Requiring device authentication for account confirmations, which never asked for it (see
  "Open decisions for owner").
- Any backend change.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `mobile-purpose-questions`: refused prompt answers stay answerable, and purpose attempts are traced
  and shown in Tanya.
- `mobile-notification-completion`: refused account confirmations stay answerable, and account
  attempts are traced and shown in Tanya.

## Impact

- Code:
  - receivers: `PurposeAnswerReceiver`, `MappingConfirmReceiver`, `SenderReplyReceiver`;
  - shared code: a new `NotificationAnswer` and `AnswerAttempts`;
  - domain stores: `PurposeAnswers`, `MappingConfirmation`, `SenderReplies`;
  - notifiers: `PurposeQuestionNotifier`, `MappingConfirmationNotifier`;
  - Tanya: `CompanionViewModel`, `OpenQuestions`, `QuestionsScreen`, the three question controls,
    and a new `UnsavedNotice`.
- No API or schema change in this follow-up.
