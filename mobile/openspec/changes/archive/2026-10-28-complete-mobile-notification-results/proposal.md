# Proposal

## Why

The companion treats HTTP batch acceptance as completed recording, even though the backend may finish an accepted notification later. It can lose success alerts, repeat alerts for a paired movement, and pass a logical result key to a ledger edit endpoint.

## What Changes

- Persist backend event identity, processing state, safe error, logical record key, and editable ordinary transaction ID separately, with explicit non-destructive Room migrations.
- Poll queued/processing results with bounded durable WorkManager recovery, preserving older accepted records and the pairing that accepted them.
- Alert only for complete committed results, deduplicated by logical record key, with correct account endpoints and no retroactive alerts on upgrade.
- Align existing status and edit controls with actual backend completion and valid transaction UUIDs.
- Remove private-content logging, restrict debug injection, and exclude credentials and raw notifications from backup and device transfer.
- Verify the upgrade, app behavior, and release manifest on the connected TECNO device using fictional local test data.

## Capabilities

### New Capabilities

- `mobile-notification-completion`: Durable result tracking, safe alerts, migration, recovery, privacy, and truthful controls.

### Modified Capabilities

None.

## Impact

- Companion Retrofit models/service, Room entity/DAO/database, WorkManager workers/scheduler, notifier, existing inbox controls, manifest/backup rules, and focused tests.
- Uses the backend's owner-scoped result endpoint and optional ordinary `transaction_id` from the coordinated backend change. No new Android library family or broad UI redesign.
- The phone upgrade preserves installed application data and credentials; no historical replay or automatic terminal retry.
