## Approval

Owner authorized phases 2–8 on 2026-10-28 without per-phase review; coordinator-reviewed.

## Why

The companion forwards everything an enabled app posts except one-time codes. In a recent device sample about four in five captured rows were promotions, market news or offers, and the tracker had to discard them. The capture path itself can lose or duplicate real transactions:

- Inserts run fire-and-forget, and errors are only logged.
- Re-posts of one notification create new rows.
- Inbox-style and messaging-style text is not read.
- One rejected row marks a whole batch of up to 50 as `invalid_request`.
- Three registered apps cannot be toggled.
- The Gojek super-app and the GoPay app report the same payment twice.

This is phase 4a of the cash-flow-tracker plan `research/notes/2026-10-28-automation-quality-plan.md`: the device must capture every real transaction and drop noise.

## Expected Outcome

- Each capture gets one of three outcomes before any network call, with a reason code on the row:
  - **Drop:** one-time codes, ongoing notifications, empty group summaries, the listener's own notifications and cross-app duplicates.
  - **Send:** recognised transactions and plausible settled money events.
  - **Keep on the phone:** promotions, market news, headline figures and amount-less text.
- Rows kept on the phone appear under "Disimpan di ponsel". The owner can send any of them with "Kirim ke Tracker". They are deleted after 7 days.
- A capture that cannot be stored is retried. If it still fails, the failure is counted and shown in Settings.
- A re-post of the same notification updates its row. Two identical payments at different times stay as two rows.
- One invalid row no longer blocks the rows sent with it.
- Every registered app can be toggled in Settings. The defaults are unchanged.
- When two apps of the same institution post the same text within 2 minutes, only one row is sent.

## What Changes

- Add an on-device capture classifier that mirrors the backend's evidence rules in `notification_evidence.py` (cash-flow-tracker): hard noise, promotional, market information, headline figures, no amount, and the recognised-transaction exemption.
- Read `EXTRA_TEXT_LINES` and `MessagingStyle` messages into the captured text. Read the ongoing, foreground-service and group-summary flags, and `Notification.when`.
- Store captures through one serial capture queue with explicit retry and failure counting. Update the existing row on a re-post with the same key, content and `when`.
- Pair cross-app duplicates one-to-one within a 2-minute window, and mark the later row as dropped with `duplicate_source`.
- Add Room schema version 6 with capture outcome, reason, posting identity, re-post count and duplicate link. Include the exported schema and a migration test.
- Submission sends only rows with the send outcome. After a 4xx batch rejection, it retries each row on its own.
- Inbox: add a "Disimpan di ponsel" filter and a "Kirim ke Tracker" action. Settings: list every registered package, and add capture-health counters.
- Version 1.7.0 (code 14).

## Non-goals

- The "Apa ini?" merchant prompt and suggestion chips (phase 4b).
- Backend changes. The request contract and the payload hash formula are unchanged.
- Installing on any device. Release follows the existing rule: the backend ships first, then the APK, on the owner's instruction.
- Changing the backend's own evidence rules. The device mirrors them conservatively and sends whenever in doubt.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `notification-capture`:
  - classification into drop, send and keep-on-phone;
  - durable capture;
  - posting identity;
  - batch isolation;
  - app toggles;
  - cross-app duplicates;
  - safety-footer wording, which is updated for classification.

## Impact

- **Code** (under `app/src/main/java/.../listener/`):
  - `service/` (new `CaptureClassifier`, `NotificationProcessor`, `FinancialNotificationListenerService`);
  - `data/db/`, `data/model/RawNotificationEntity`;
  - `data/sync/NotificationSubmission`;
  - `data/prefs/AppPreferences`;
  - `registry/FinancialSourceRegistry`;
  - `ui/inbox/*`, `ui/settings/SettingsScreen`, `ui/components/StatusBadge`;
  - the debug receiver.
- **Database:** Room 5 → 6 with additive columns and indexes. Existing rows keep the send outcome and their behaviour.
- **Tests:**
  - new JVM unit tests for the classifier, posting identity, duplicates, batch isolation and retry, plus Robolectric tests for the store, the processor and the migration;
  - the instrumented migration test is extended.
- **Dependencies (test only):** Robolectric, AndroidX Test core and AndroidX JUnit for JVM tests.
- **Release:** 1.7.0 (code 14). It depends on no backend change.
