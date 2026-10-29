## Approval

This is a corrective change found on 2026-10-29 while checking the owner's defect report, under the owner's 2026-10-28 instruction to continue until the plan is done.

## Why

Completing a one-sided transfer on Tinjau creates the missing leg. The bank's own notification for that leg can arrive later: the companion was frozen, or a Jago source-pocket question is still pending.

- If that notification arrives more than 30 seconds after the completed leg's time, it is recorded again. This was reproduced: a 10-minute-late Jago leg made a second Rp 500.000 expense.
- If it arrives sooner, it is held as `ambiguous_existing_movement`, which leaves an unresolved review item.

In production, one completed transfer (28 Oct, Main Pocket → BCA ATM) has exactly such a pending Jago notification.

## Expected Outcome

A notification for a leg the owner already completed is attached to that transfer as its confirmation, so no second row is recorded. With two candidate completed legs, it is held for review and not guessed.

## What Changes

When an observed leg is recorded, the system first looks for a completed leg (`movement-complete:` key) with:
- the same account, direction and amount;
- a time within the pairing window;
- no notification of its own.

One match records the event as a `role_confirmation` of that movement; two raise `ambiguous_existing_movement`.

## Non-goals

- Other duplicate paths. Notification legs are already deduplicated by payload hash and pairing.

## Capabilities

### Modified Capabilities

- `movement-pairing`: adds a requirement for late legs of completed transfers.

## Impact

- **Code:** `backend/app/services/notification_application.py`, plus tests.
- **Database and API:** no migration and no API change.
