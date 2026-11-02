# Proposal

## Why

An unnamed Jago outbound transfer needs an explicit source choice before the backend can debit the correct pocket. The companion must make that choice quick and deliver it safely across offline periods.

## Expected Outcome

The owner taps `Pilih kantong`, selects an eligible registered Jago pocket without a preselected default, and confirms. The app retains the choice until the backend records or returns an actionable review state.

## What Changes

- Adopt the mobile portion of the owner-approved backend change `confirm-jago-outbound-source-pocket` in the cash-flow-tracker repository (`openspec/changes/archive/2026-10-30-confirm-jago-outbound-source-pocket`).
- Add typed source-pocket results/options/answers, notification and inbox controls, current/cached options, and durable pairing-scoped delivery.
- Migrate existing reply rows and scheduled sender work safely; preserve sender and legacy mapping behavior.

## Capabilities

### New Capabilities

- `mobile-jago-pocket-confirmation`: Complete pocket list, explicit selection, durable answers, authoritative completion and compatibility.

### Modified Capabilities

None.

## Impact

Network models/API, result validation, notification routing, inbox sheet/view model, reply outbox/worker, Room 5 migration and companion version 1.6.0/code 12. Existing installed source is 1.5.0/code 11 at `b64cd6e`. Verification uses fictional local-backend data and the separate verification application ID; normal installation follows deployed backend evidence.

This adoption does not expand the approved contract or authorize direct production access, historical ledger correction, or destructive app-data changes.
