## Context

See `proposal.md` for the motivation. The base is `origin/main` at `afcc0db` (1.8.0, code 15,
Room 7). The facts this design relies on:

- The ingest request (`NotificationEventRequest`, backend `routers/ingest.py`) has no account or
  category fields. Its label fields (`event_class`, `expected_amount`, `expected_direction`,
  `expected_counterparty`, `label_notes`) are stored as labels. `notification_evidence.py` maps
  `expected_direction` (`out`, `in`, `internal`) and flags a mismatch with the parsed direction as
  `conflicting_mobile_direction`.
- `NotificationSubmission.toRequest` sends a `transfer` row with a blank counterparty as
  `income`.
- On 1.8.0, `saveAndSyncUnsynced` and `updateSyncedTransaction` wrote `expectedCounterparty = null`
  and a direction of `in` or `out`, and `saveAndSyncUnsynced` ignored its `accountId` and
  `categoryId` arguments.
- On 1.8.0, the settings switch only wrote the preference. `FinancialTrackerApp` scheduled the
  periodic work with `KEEP`, so it survived the switch, and `onListenerConnected` started
  `DirectSync` and an immediate sync without checking it.
- The owner's phone runs the debug build (`.debug` application ID) against the HTTPS tracker.

## Goals / Non-Goals

**Goals:** remove code nothing uses, give production code domain names, deny cleartext in
release, fix the two audit defects with regression tests, close the old OpenSpec changes, and make
`openspec validate --all --strict` pass.

**Non-goals:** see `proposal.md`. No Room migration, no backend change, no device installation.

## Decisions

### D1. Remove only what search and the compiler prove unused

Each declaration in the network service, the models, the DAOs, the inbox view model and the
preferences was counted across `main`, `debug`, `test` and `androidTest` with a word-boundary
search. A candidate is removed only when no other file names it and the build still compiles.

Removed:

| Item | Evidence |
| --- | --- |
| `IngestionApiService.createTransaction`, `createMovement` | No caller in any source set. They predate the ingest pipeline (`5159eb9`). |
| `TransactionCreateRequest`, `TransactionResponse`, `MovementRequest`, `MovementResponse` | Used only by the two methods above. |
| `NotificationDao.getNotificationsByPackageFlow`, `deleteEmptyNotifications`, `getByPayloadHash`, `nextCompletionAt`, `markAsSynced`, `updateServerTransactionId`, `updateSyncError`, `deleteDebugRecords` | No caller in any source set. |
| `NotificationDao.purgeOldSynced` | No production caller. One instrumented test called it next to the production retention queries. That test now calls `pruneIgnoredBefore`, the query production runs, so it still checks that retention keeps a row with a pending reply. |
| `NotificationDao.updateLabel` | Replaced by `saveOwnerLabel` (D6). |
| `DebugInboxViewModel.saveLabel` | No caller. |
| Stale imports in `MainActivity`, `SettingsScreen`, `Theme`, `NotificationDao`, `NotificationProcessorTest`, `SenderReplyPersistenceTest`, and the moved `IngestionApiService` imports | An import scan found no use of the imported name. |

Kept on purpose:

- `NotificationDao.deleteById`: no production caller, but six instrumented tests use it to clean up.
- Response fields that the app does not read (`inserted`, `created_transactions`, `mapped`,
  `total_balance`, `parent_id`, `default_pocket_id`): they mirror the server contract, and
  `IngestResultItem` is stored as `result_json`, so dropping fields would change stored data.
- `TextSecondaryLight`: part of the light palette next to its used siblings.
- `CaptureReason.OWNER_SENT`, `ProcessingReasons.hasMessage`, `SenderReplyWorker`'s class identity:
  used by tests or by persisted WorkManager jobs.

Rejected: removing everything without a production caller. That would delete test support and
stored contract fields for no runtime gain.

### D2. Keep the `server_transaction_id` column

No code writes it since commit `7bf48c9` (database version 3). Room validates entities against
the table, so the column needs its entity field. The field stays, with a comment that names its
replacements (`editable_transaction_id`, `record_key`). The instrumented status test keeps setting
it, because it checks that a legacy logical key never enables an ordinary edit.

Rejected: a Room 8 migration that rebuilds `raw_notifications` without the column. It risks the
owner's stored rows for no behaviour gain.

### D3. Domain names for the inbox

`DebugInboxScreen` and `DebugInboxViewModel` become `InboxScreen` and `InboxViewModel`, files
moved with `git mv`. Nothing else changes. `DebugNotificationReceiver` stays in `src/debug`, so
release builds still declare no simulation receiver.

### D4. Cleartext through a network security configuration

`main/res/xml/network_security_config.xml` sets `cleartextTrafficPermitted="false"`. A file with
the same name in `src/debug/res/xml` sets it to `true` and replaces the release one in debug
builds only. The manifest references the configuration instead of setting
`usesCleartextTraffic`, which Android ignores once a configuration is present (API 24+, and
`minSdk` is 26).

Rejected:

- A manifest placeholder for `usesCleartextTraffic`: it works, but the configuration is the
  documented place for transport policy and can later hold trust anchors or pins.
- Allowing cleartext for named LAN hosts in release: the hosts are not known, and the tracker is
  HTTPS.

The owner's phone runs the debug build, so its behaviour is unchanged. A release build pointed at
an `http://` address now fails to connect and keeps its rows pending.

### D5. The send form offers only what the send carries

For a row that has not been sent, the sheet hides the account and category pickers and says that
the tracker picks them when it records the notification. The pickers stay for a recorded ordinary
transaction (the PATCH carries them) and for a reviewable row (the manual record carries them).
`onSaveUnsynced` and `saveAndSyncUnsynced` lose their `accountId` and `categoryId` parameters, so
the choice can no longer be dropped silently.

Rejected:

- Adding account and category hints to the ingest request: a backend contract change outside this
  phase. Recorded as an open decision.
- Storing the choice and applying it after the result arrives: a new persisted state machine for a
  rare path.

### D6. Owner labels keep what the owner did not change

`OwnerLabels` (in `data/capture`) holds the rule, and the view model calls it:

- `labelForSend` refuses a row that was sent, joins a kept row to the send pipeline (`sendLocal`),
  and stores the labels.
- `labelEdited` stores the labels after the server accepts a ledger edit.

Both call the new `NotificationDao.saveOwnerLabel`. It sets `is_financial`, `event_class`,
`expected_amount`, `expected_direction`, `label_notes` and `labelled_at`, and does not touch
`expected_counterparty`. The direction is `out` for expense, `in` for income, and the captured
direction for any other type. The sheet only offers expense and income, so another type is always
the captured one, such as a pocket move's `transfer`.

Rejected: keeping `updateLabel` and passing the stored counterparty back in. Every caller would
have to remember to do so, and the defect was exactly a caller passing `null`.

### D7. One owner for automatic sending

`AutoSync` (in `data/sync`) owns the switch:

- `setEnabled(true)` stores the switch, schedules the periodic sync and queues an immediate send.
- `setEnabled(false)` stores the switch and cancels the periodic sync and the immediate send.
  Without that cancel, an immediate send that is waiting for the network or retrying would still
  run.
- `restore` runs at app start. It schedules the periodic sync, or cancels it when the switch is
  off. This also removes periodic work that older builds left scheduled.
- `sendPending` starts `DirectSync` and an immediate send only when the switch is on and an
  account is connected. Capture and listener reconnect both call it.

Owner actions still call `SyncScheduler` directly: "Sinkronkan sekarang", sending a kept row,
resending an ignored row, and answers. Completion lookups for events the server already accepted
are not sends, so they continue.

Rejected: checking the switch inside `NotificationSyncWorker`. The worker cannot tell an owner's
"sync now" from an automatic run without new input data, and cancelling the work already stops it.

Side effect: turning the switch off also cancels a "Sinkronkan sekarang" that is still waiting for
the network. The rows stay pending and visible, and the owner can sync again.

### D8. Splitting over-long requirements

The old changes are archived first, in dependency order: completion (Room 3), sender replies
(Room 4), then Jago pockets (Room 5). Their capabilities therefore exist as main specs. For each
requirement over 500 characters, this change's delta:

- keeps the requirement under MODIFIED with its header and every scenario, and cuts its
  description to one behaviour without changing it;
- adds each behaviour it removed as an ADDED requirement with the same normative text and at least
  one scenario that illustrates it.

No requirement text is reworded beyond the move. The new scenarios restate behaviour that the
original text already required.

### D9. Version 1.8.1 (code 16)

`PurposeAnswersTest` asserts the `X-Companion-Version` header as a literal. It is updated to
`1.8.1`, so a stale build constant still fails the test.

## Data, API and failure behaviour

- No API, request or response change, and no new endpoint.
- No Room schema change; the database stays at version 7, and `schemas/7.json` is unchanged.
- A failed `saveOwnerLabel` reports "Gagal menyimpan label" as before. A row that was sent
  meanwhile is refused with the existing message and is not relabelled.
- With auto-sync off, captures stay pending until the owner syncs or turns it on.

## Migration and rollback

There is no data migration. Rollback means reinstalling the 1.8.0 APK with a downgrade install,
which keeps the app data: the schema is identical, and the new code writes no new values. After a
rollback, an auto-sync switch that is off goes back to the old leak until 1.8.1 is installed again.

## Open decisions for owner

1. Should the ingest contract accept account and category hints, so an unsent notification can
   carry the owner's choice? This needs a backend change. Until then, the sheet does not offer them.
2. A release build now refuses `http://` server addresses. That is fine while the phone runs the
   debug build against HTTPS. Confirm that no release build points at a plain-HTTP address.
3. Should a later Room migration drop `server_transaction_id`? This change keeps it.
4. Turning auto-sync off also cancels a manual "Sinkronkan sekarang" that is still waiting for the
   network. Is that acceptable, or should manual syncs get their own work name?
