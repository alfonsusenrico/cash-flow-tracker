## Why

The owner's Sesi uji of 2026-11-05 surfaced a regression from `catch-second-report-after-pairing`, replayed on `main` `dd67c58`.

Step 10 was a second BCA → GoPay top-up of the same amount, 98 seconds after step 7. This time, Jago's "You've received … from GoPay" arrived before GoPay's own report.
- **The link declined, as it should.** The owner's Gopay ↔ Jago link would not match the Jago report to step 7's GoPay credit, because that credit had already absorbed its own Jago report (absorb-once, OD133).
- **The unlinked guard matched it anyway.** Since D1 of that change, the unlinked guard also matches paired legs, and it has no absorb-once rule. It held the report as a second observation of step 7's credit.

No money was double-booked. But a "Terlihat dua kali" item points at the wrong top-up where the link should have skipped the report, and without a link one top-up's report can be absorbed by another's leg.

## Expected Outcome

- **One report per app:** a recorded leg absorbs at most one report from each other app, with or without a link. A second report of the same kind is decided against its own top-up.
- **The Sesi uji replay:** step 10 shows one movement and one report skipped by the link. All 18 steps score as expected.

## Approval

On 2026-11-05 the owner said "ok create the plan and do fix", after the Sesi uji report that named this regression.

## What Changes

- **`SAME_ARRIVAL_SQL`:** it returns each leg's absorbed report packages, as `LINKED_REPORT_SQL` does.
- **`same_arrival`:** it ignores a leg that has already absorbed a report from the new leg's app.

## Non-goals

- No change to links, windows, the naming rule, or the swap and possible-transfer rules.
- No migration, no Android change and no data repair. The only affected events were Sesi uji captures, which book nothing.
- The companion's "Di ponsel" sorting of the two Jago → ShopeePay notifications is a separate follow-up.
- No production action beyond read-only checks. The release goes through the merge and the deploy workflow.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `movement-pairing`: "One Arrival Seen by Two Apps" gains the one-report-per-app condition and its scenario.

No project capability is added.

## Impact

- **Code:** `backend/app/services/movement_pairing.py`, with tests in `backend/tests/test_duplicate_arrivals.py`.
- **API and data:** no API, schema or data change.
