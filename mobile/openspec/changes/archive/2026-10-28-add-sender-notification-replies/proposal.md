# Proposal

## Why

The companion needs to let the owner name unfamiliar masked BCA transfer senders before ledger recording. This adopts the already approved mobile portion of Cash Flow Tracker's `add-notification-sender-clarification` change; it introduces no additional product scope.

## Expected Outcome

The owner can reply with a name directly from a sender question or choose `Catat tanpa nama`. Offline answers survive restart, route to the correct event and pairing, and produce success only after the backend commits a single ledger record.

## What Changes

- Consume typed sender questions and question-specific alert identities while preserving legacy account actions.
- Add restricted Android direct reply actions and equivalent detail-sheet controls.
- Add a non-destructive Room reply outbox and WorkManager delivery/recovery.
- Preserve pairing, financial records, privacy and truthful completion states during upgrade.

## Capabilities

### New Capabilities

- `mobile-sender-replies`: Inline and in-app sender answers with durable, question-bound delivery.

### Modified Capabilities

None. Existing mobile-completion behavior is preserved; sender questions extend it.

## Impact

API models, result consumption, notification actions/receiver, Room schema, reply worker, inbox detail sheet, tests and version 1.5.0/code 11. Existing AndroidX/Room/WorkManager dependencies suffice. Backend deployment precedes normal-app installation. Canonical cross-repository plan: Cash Flow Tracker `openspec/changes/add-notification-sender-clarification/`, approved by the owner on 2026-10-05. The adopted design/specs are included here for native companion apply.
