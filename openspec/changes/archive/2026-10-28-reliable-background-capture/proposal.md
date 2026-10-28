# Proposal

## Why

On the owner's TECNO (HiOS, Android 16) the companion only processes captured notifications reliably while its screen is open. Evidence from 2026-09-30: a Stockbit notification posted at 03:01 was captured at 07:23; HiOS's power manager logged `p_job`/`p_alarm` interception for the app (uid 10176) although it is Doze-exempt and in the exempt standby bucket; `background_power_saving_enable=1`. Sync and result checks run as WorkManager jobs, which HiOS holds back, so records and "Tercatat" alerts appear late or not at all. The ongoing listener notification also disappears without explanation, and there is no on-device record of listener disconnects to diagnose this.

## What Changes

- After the listener stores a notification, the app submits pending events and checks their results directly in the listener's process, instead of only enqueueing WorkManager jobs. WorkManager stays as the retry fallback.
- One submission runs at a time in the process; direct and scheduled paths share the same code.
- Listener health is recorded on the device: connect and disconnect times, disconnect count, last capture, and last direct sync outcome. The Settings screen shows them.
- When the listener is disconnected, the app asks Android to rebind it (on disconnect and when the app is opened).

## Capabilities

### New Capabilities
- `mobile-background-capture`: prompt submission and result checks without relying on deferrable background jobs, plus listener health diagnostics and rebinding.

### Modified Capabilities
None.

## Impact

- Mobile only (`financial-tracker-mobile-listener`): new `NotificationSubmission` and `DirectSync`, `NotificationSyncWorker` refactor, `NotificationProcessor`, `FinancialNotificationListenerService`, `MainActivity`, `AppPreferences`, `SettingsScreen`; version 1.4.0.
- No backend, schema, or Room migration changes.
