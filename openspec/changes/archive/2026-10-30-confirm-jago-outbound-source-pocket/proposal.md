# Proposal

## Why

Some settled Jago outbound transfer notifications omit the source pocket. The released backend currently resolves that missing source to the main pocket, so an otherwise correctly parsed transfer can reduce the wrong balance; the owner should choose the actual pocket before the event is recorded.

## What Changes

- Hold a recognized Jago outbound transfer when its notification provides no explicit source-pocket evidence. Neither high AI confidence nor a matching incoming transfer may substitute for that evidence.
- Return a typed, event-scoped source-pocket question. Let the owner choose from all currently eligible, registered Jago pockets, including the main pocket, without preselecting one.
- Add an authenticated, idempotent confirmation API. Persist the choice for this event and resume the existing recording/pairing pipeline; do not create a remembered account alias or a default for later unnamed transfers.
- Add a notification action, `Pilih kantong`, that opens a compact Android chooser, plus the same controls in the inbox detail sheet. Queue confirmations durably when offline and show pending status until the backend actually records the transaction.
- Preserve named Jago pocket movements, other banks, BCA sender replies, paired internal transfers, pairing credentials, and existing records. Older clients safely receive review status rather than silently recording an unnamed Jago source.
- Verify with fictional transactions and the connected USB phone using the separate local-backend verification app. Prepare backend-first delivery and a companion upgrade after implementation approval and backend deployment.

## Capabilities

### New Capabilities

- `mobile-jago-pocket-confirmation`: Android notification/inbox pocket selection, current eligible options, durable delivery, truthful state, and upgrade compatibility.

### Modified Capabilities

- `notification-ingestion`: Add source-pocket confirmation for unnamed Jago outbound transfers, with owner-scoped candidates, event-specific answers, write guards, compatibility, and pairing invariants.

## Impact

- Backend: existing evidence/resolution, context, processing, application and ingestion API services; additive event-decision and answer-receipt persistence through the next available Flyway migration and mirrored startup schema; targeted parser/application/API/PostgreSQL regressions.
- Companion: the separate `financial-tracker-mobile-listener` repository, currently delivered from `/private/tmp/cft-mobile-sender-replies`; network result/request models, notification routing, inbox sheet/view model, typed durable confirmation delivery, Room migration, tests and APK version. Adopt the approved mobile contract into a native companion change before source edits.
- No new framework, paid model benchmark, general parser/model change, web UI, recurring schedule, automatic pocket learning, production data inspection, or historical balance correction is included. Recorded events are not reopened automatically.
- The original backend checkout contains unrelated edits and older source. Implementation must use isolated topic branches from the refreshed target and preserve that checkout. Backend code is delivered by the owner-created/merged PR and designated deployment pipeline; this plan does not authorize a main push, PR creation, production-server access, or immediate device installation.

## Review boundary

This is the proposed contract, not implemented behavior. Approval is required before source edits. Release evidence and owner acceptance remain separate from OpenSpec artifact validation.
