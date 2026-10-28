# Design: reliable-background-capture

## Context

See proposal.md. Capture already happens inside `NotificationListenerService` callbacks, which run in a process kept at foreground-service priority (process state 4 observed). Everything after capture (batch submit, result polling, alerts) is scheduled through WorkManager, which HiOS intercepts (`TranUsfAppInternal: p_job`). Whether the overnight capture gap was a frozen process or an unbound listener is not established; the device log retained only four hours.

## Goals / Non-Goals

Goals: a captured notification is submitted and its result checked within seconds of capture when the network is available; failures fall back to WorkManager; listener disconnects leave evidence on the device.

Non-Goals: defeating OEM freezing of a process that the OS has frozen (not possible from an app); changing the backend; changing capture rules.

## Decisions

### D1. Shared submission with a process-wide mutex
`NotificationSubmission.submitPending()` holds the batch logic now in `NotificationSyncWorker.doWork()` and returns an outcome (`Nothing`, `Submitted`, `Retry`, `Stop`). The worker maps it to `Result`. A companion `Mutex` serialises submissions from both paths; the backend is idempotent per payload hash, so a duplicate submission is harmless.

### D2. Direct sync after capture
`DirectSync.trigger(context)` runs on an application-scoped `SupervisorJob` IO scope: submit pending events, then for events still queued or processing, poll `GET /api/ingest/notifications/{id}/result` at 5, 10, 20, 40 and 60 seconds (about 2 minutes 15 seconds total), consuming results and delivering alerts as they complete. Only one direct run is active at a time; a trigger during a run marks it dirty so the run repeats once. On any network failure the run stops and the existing WorkManager enqueue remains the fallback. `NotificationProcessor` keeps enqueueing WorkManager work as before and also triggers direct sync.

### D3. Listener health record
`AppPreferences` stores `listenerConnectedAt`, `listenerDisconnectedAt`, `listenerDisconnectCount`, `lastCaptureAt`, `lastDirectSyncAt`, `lastDirectSyncOutcome`. The listener updates them in `onListenerConnected`/`onListenerDisconnected`; the processor updates `lastCaptureAt`; direct sync updates its fields. Settings shows a "Status Listener" card with these values in local time.

### D4. Rebinding
`onListenerDisconnected` calls `requestRebind(ComponentName)`. `MainActivity.onResume` calls it when the permission is granted but the listener reports disconnected.

## Risks / Trade-offs

- [HiOS freezes the whole process] → direct sync cannot run while frozen; the health record will show it (captures stop, then resume with a gap), which tells the owner to change HiOS settings rather than hide the problem.
- [Polling while the screen is off costs battery] → bounded to about 2 minutes per capture burst, only while events are pending.

## Migration Plan

Install 1.4.0 over 1.3.2 (no Room change). Rollback: reinstall the saved 1.3.2 APK.

## Open Questions

- Cause of the overnight gap (frozen vs unbound): answered by the health record after the next night.
