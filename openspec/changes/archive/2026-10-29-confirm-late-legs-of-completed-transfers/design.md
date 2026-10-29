## Context

See proposal.md. The leg that completion creates copies the other leg's date and carries the key `movement-complete:<id>`. Before this change, when an observed leg was recorded:
- the 30-second role-confirmation check found no notification event for that leg;
- the counterpart search ignored legs that are already linked.

So a late leg was recorded again.

## Decisions

### D1. Check completed legs before everything else in `record_observed_leg`

A completed leg matches when it has:
- the same account, type and amount;
- `|date - post_time| <= NOTIFICATION_PAIRING_WINDOW_SECONDS`;
- a linked movement;
- no `notification_events.transaction_id` pointing at it.

The rows are locked and the nearest come first.

| Matches | Result |
|---|---|
| One | The event is saved as `role_confirmation` for that leg and movement, with an internal-movement snapshot. |
| Two or more | `ambiguous_existing_movement`: the event is held and no row is written. |

*Rejected:* widening the existing 30-second window. That window serves Jago complete-movement notifications, whose two reports arrive seconds apart.

*Rejected:* re-pairing after recording. A duplicate would have to be detected and deleted after the fact.

## Risks / Trade-offs

- [A genuine second transfer of the same amount on the same account within 15 minutes of a completed one] → It is attached to the completed transfer instead of being recorded. This is rare. The owner sees one movement and can split it, which the existing split flow supports.

## Migration Plan

Deploy through the pipeline. Nothing is backfilled. The production event that is already pending resolves correctly when it is next processed (after the owner answers its pocket question).
