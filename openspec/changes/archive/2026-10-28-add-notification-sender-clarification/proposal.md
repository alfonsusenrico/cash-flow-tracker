# Proposal

## Why

BCA transfer notifications can mask the sender's name even when the owner knows who sent the money. Today the parser can record a generic description immediately, losing an opportunity to clarify the sender and remember the owner's answer.

## Expected Outcome

An incoming BCA transfer with an unfamiliar masked sender stays pending until the owner replies with a name or explicitly chooses to record without one. The companion offers an inline notification reply and the same controls in its detail sheet. A confirmed name produces a description such as `Transfer masuk dari Andra`; later transfers with the same account-scoped mask reuse that confirmation. Offline replies survive restarts, and repeated replies never create another transaction.

## What Changes

- Add sender clarification before any ledger write, independently of account-mapping and financial confidence; preserve existing evidence checks and movement pairing.
- Extend notification results with typed, revision-bound sender questions; retain the existing account-confirmation contract.
- Add an owner-scoped answer endpoint with durable, idempotent acceptance and an explicit `Catat tanpa nama` choice.
- Store confirmed sender aliases separately from owner identity and account aliases; provide bounded AI context and a Settings list with correction/removal controls.
- Add Android direct replies, a persistent reply outbox, retry states, and in-app recovery when a notification is dismissed or disabled.
- Gate sender questions by the new companion version and keep older builds actionable through `needs_review`.

## Capabilities

### New Capabilities

- `notification-sender-clarification`: Pending sender questions, validated owner answers, exact scoped alias reuse, memory management, and compatibility.
- `mobile-sender-replies`: Inline replies and in-app sender confirmation with durable delivery, truthful status, and question-specific routing.

### Modified Capabilities

None. Existing durable ingestion/filtering specs lag the already released notification pipeline; this additive change does not rewrite their unrelated legacy transfer/filtering requirements. Its sender gate extends the current implementation and the completed, unarchived account-mapping change.

## Impact

- Backend: ingestion result/API contracts, processing/application guards, context/prompt, versioned PostgreSQL migration and matching startup schema, owner-scoped Settings APIs, and focused tests.
- Web: existing `SettingsModal`, typed API/query keys, and tests for sender-memory correction/removal.
- Companion repository: `../financial-tracker-mobile-listener`; typed results, notification actions/receiver, Room migration/outbox, WorkManager delivery, detail sheet, and tests. This repository holds the coordinated plan; companion-native artifacts will adopt its mobile contract before implementation there.
- Release: deploy backend through the existing owner-merged topic-branch pipeline, then upgrade the app while preserving pairing and local data. USB-device verification uses fictional events against a local backend.
- Existing Room, WorkManager, and AndroidX notification dependencies suffice; no model change or paid general AI benchmark is included.
- Non-goals: open-ended AI chat, inferring identities from masks, retroactively renaming recorded transfers, contacts import, automated pending expiry, production data repair, or fixing the previously diagnosed OEM background freezer.
