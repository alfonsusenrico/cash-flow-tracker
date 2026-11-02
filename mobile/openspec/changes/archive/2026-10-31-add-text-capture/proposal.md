# Proposal

## Approval
Owner approved the companion redesign plan with defaults on 2026-10-29 (research/notes/2026-10-29-companion-redesign-plan.md); coordinator-reviewed. The defaults include "Hapus dari tracker" (`DELETE /api/transactions/{id}` after a confirmation) only for transactions created by a capture. On 2026-10-30 the owner reconfirmed: "I want all plans executed, implemented properly". This change is phase 3 of that plan. It ships as 2.3.0, code 22, because 2.2.0 is already used by the merged ledger summary.

## Why
The owner records cash, small payments and own-account moves that no bank notification reports. The deployed backend accepts one typed Indonesian sentence, asks only for what it cannot work out, and suggests what the owner usually records at this hour. The phone has no way to use it.

## Expected Outcome
A raised "Catat" button opens a capture sheet. The owner types a sentence or picks a suggestion from their own history, and the tracker records it or asks one question at a time. A recorded capture can be undone for 10 seconds, edited, and later deleted from the tracker after a confirmation. A capture is stored on the phone before it is sent, so going offline, a server error or a killed process never loses or duplicates it.

## What Changes
- A centred, raised Catat button in the bottom bar's reserved slot and a capture sheet: input, history suggestions, processing, success with Ubah and a 10-second "Batalkan", follow-up questions, queued, error and undone states.
- Room v8 adds a `text_captures` table. The 7→8 migration creates only that table.
- Captures are sent at once and resent through WorkManager with a network constraint, under the auto-sync switch. `client_capture_id` and answer `reply_id` values make every resend idempotent.
- Captures appear in Aktivitas as "Diketik di ponsel", own-account moves as "Pindah saldo", and in the home figures that come from this phone. Server ledger totals are never mixed with local figures.
- Rincian for a capture offers "Hapus dari tracker" behind a confirmation, only for an expense or income the capture created. Notification rows keep the local "Hapus dari aplikasi".
- Version 2.3.0, code 22.

## Non-goals
- Backend changes, web capture input, notification parsing rules, and habit learning (phase 9).
- Answering capture questions in Tanya. A capture's question is answered in its sheet, which Aktivitas and Rincian reopen.
- Deleting a captured own-account move from the phone. The move endpoint is not part of the approved contract, so the phone points to Tracker Web.
- Installing on the owner's TECNO, or any production access.

## Capabilities
### New Capabilities
- `mobile-text-capture`: Typed capture on the phone: the sheet, suggestions, durable queue, questions, undo, edit, delete from the tracker, and where captures appear.
### Modified Capabilities
- `notification-capture`: The auto-sync requirement also governs the automatic resend of queued captures.
- `companion-interface`: The home figures requirement counts recorded captures and says the figures come from notifications and captures on this phone.

## Impact
- Android: Room entity, DAO and migration 7→8 with exported schema 8; capture repository, worker and API models; bottom bar, capture sheet, capture Rincian, Aktivitas, Beranda; the edit form takes its initial values from either a notification or a capture.
- API (read-only contract, cash-flow-tracker origin/main 3310e24): `POST /api/capture/text`, `POST /api/capture/{id}/answer`, `POST /api/capture/{id}/undo`, `GET /api/capture/{id}`, `GET /api/capture/suggestions`, `PATCH /api/transactions/{id}` and `DELETE /api/transactions/{id}`.
- No new dependencies. Upgrade keeps every Room row, the key, the pairing context and preferences.
