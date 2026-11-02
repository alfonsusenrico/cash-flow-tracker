# Proposal

## Approval

Owner authorized phases 2–8 on 2026-10-28 without per-phase review; coordinator-reviewed.

This is phase 8b (companion cleanup) of the cash-flow-tracker plan
`research/notes/2026-10-28-automation-quality-plan.md`.

## Why

The companion carries code that nothing calls, a production screen still named "Debug", and a
manifest that allows plain HTTP in every build. The 2026-10-28 audit also found two defects that
are still on `main` (1.8.0):

- Sending an unsent notification from the inbox ("Kirim Notifikasi ke Tracker") shows account
  and category pickers, then throws the choice away. It also erases the captured counterparty and
  rewrites the direction to "out". For a Jago pocket move, submission then turns the label
  "transfer" into "income", and the backend flags the event `conflicting_mobile_direction`.
  Saving a ledger edit erases the counterparty the same way.
- Turning "Auto-Sync Notifikasi" off only stops sends after a capture. The 15-minute periodic
  sync keeps running, and every listener reconnect still sends.

## Expected Outcome

- The send form for an unsent notification offers only what the send carries: type, amount and
  note. It says that the tracker picks the account and category when it records the event.
- Owner edits keep every captured label they do not change, including the counterparty and the
  direction of a pocket move.
- With auto-sync off, nothing is sent unless the owner asks. Turning it on resumes sending.
- Release builds refuse plain HTTP. Debug builds still allow it, for a local backend or the
  emulator.
- The production inbox has domain names, unused API methods, models, DAO queries and imports are
  gone, and the remaining specs pass `openspec validate --all --strict`.

## What Changes

- Remove unused code, each item proven unused by search and the compiler: the
  `createTransaction` and `createMovement` API methods and their four request and response models,
  nine DAO queries, the view model's `saveLabel`, and stale imports.
- Keep the legacy `server_transaction_id` column, which Room's schema needs, and document that no
  code writes it since database version 3.
- Rename `DebugInboxScreen` and `DebugInboxViewModel` to `InboxScreen` and `InboxViewModel`, with
  no behaviour change. The debug-only simulation receiver stays in the debug source set.
- Replace `usesCleartextTraffic="true"` with a network security configuration: cleartext denied in
  `main`, allowed by a debug-only override.
- Fix the inbox send and edit labels, with regression tests.
- Make the auto-sync switch cancel and restore scheduled work, and gate reconnect sends on it, with
  regression tests.
- Split the over-long requirements of four capabilities without changing their meaning.
- Version 1.8.1 (code 16).

## Non-goals

- Sending the owner's account or category with an unsent notification. The ingest contract has no
  such fields; that needs a backend change.
- Dropping the `server_transaction_id` column, which would need a table rebuild for no gain.
- Removing response fields that the server sends and the app stores in `result_json`.
- Certificate pinning or other transport changes beyond cleartext.
- Installing the build on a device.

## Capabilities

### New Capabilities

- `companion-transport-security`: which transports the companion's builds may use to reach the
  tracker.

### Modified Capabilities

- `notification-capture`: the auto-sync switch governs every automatic send, and owner edits from
  the inbox keep captured labels they do not change.
- `mobile-notification-completion`: two over-long requirements split, no behaviour change.
- `mobile-sender-replies`: three over-long requirements split, no behaviour change.
- `mobile-purpose-questions`: five over-long requirements split, no behaviour change.

## Impact

- Code: `IngestionApiService`, `ApiModels`, `NotificationDao`, `RawNotificationEntity` (comment
  only), the inbox screen, view model and sheet, `SyncScheduler`, a new `AutoSync` and
  `OwnerLabels`, `NotificationProcessor`, the listener service, `FinancialTrackerApp`,
  `SettingsScreen`, the manifest and network security configuration.
- Room stays at version 7; no migration, no data change.
- Build: `testImplementation` of `androidx.work:work-testing:2.9.1`, the version the instrumented
  tests already use.
- No backend or API contract change. Release order is unaffected.
