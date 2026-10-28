Release direction: the owner authorized backend main push and mobile install, with AI benchmarks deferred and operational AI disabled.

# Proposal

## Why

The companion currently treats batch acceptance as finished syncing, but the approved backend AI path completes asynchronously. Queued events can therefore lose their success notifications, while redelivery and paired movement events can produce duplicate alerts; the current edit flow also confuses logical record keys with transaction UUIDs.

## What Changes

- Finish backend regression/quality verification first, then implement and test the companion against the settled result contract.
- Persist acceptance, backend event ID, processing status, safe error, logical record key, and actual editable transaction identity separately; add non-destructive Room migrations.
- Poll accepted queued/processing events through the existing owner-scoped result endpoint, with bounded WorkManager recovery and no automatic replay of terminal events.
- Deduplicate/upsert verified alerts by logical record key; show source for expense, target for income, and both endpoints for movement. Never alert for acknowledgement alone.
- Add an optional actual `transaction_id` for committed ordinary results, separate from `record_key`; never PATCH a logical key or edit one movement leg as an ordinary transaction.
- Preserve unresolved records during pruning, existing phone data/configuration, listener filtering, and the approved compact payload.
- Remove private-content logging, prevent release debug-injection/purge access, and correct credential/raw-notification backup exclusions.
- Adopt the approved plan in the companion's own native OpenSpec root before implementation; this backend-root change is the cross-project coordination proposal, not permission to bypass repository or filesystem boundaries.

## Capabilities

### New Capabilities

- `mobile-notification-completion`: Durable companion acceptance/completion tracking, safe verified alert delivery, migration, recovery, and privacy boundaries.

### Modified Capabilities

- `notification-ingestion`: Explicit separation of optional ordinary ledger transaction identity from logical notification record keys, for safe companion edits.

## Impact

- Backend: compact result construction and ingestion/result contract tests; no additional database migration or provider/model change is expected. Depends on the approved `add-ai-notification-processing` change, whose final quality gate and artifact smoke remain open.
- Companion target: `/Users/enrico/enrico/project/enrico/financial-tracker-mobile-listener`, currently branch `fix/mobile-listener-reliability-and-api-alignment`, commit `6a542b4`. A new topic branch, local handoff, and explicit native OpenSpec initialization/adoption precede source changes.
- Companion code: Retrofit models/service, Room entity/DAO/database, sync scheduler/worker, transaction notifier, existing inbox edit/status controls, manifest/debug source set, backup XML, and focused tests. Reuse installed Kotlin/Room/WorkManager/Retrofit versions; only targeted test dependencies if needed.
- Verify on the connected TECNO device only after backend gates pass, using local disposable test identity/data. Existing owner notifications remain dry-run-only unless separately authorized for ingestion; no production changes, commits, pushes, or release are included.
