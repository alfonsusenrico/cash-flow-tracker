# Proposal

## Approval
Pending. Requested by the owner on 2026-11-16 ("ok plan the companion tags display") after `transaction-tags` shipped in tracker PR #119.

## Why
A sentence typed in the companion's Catat can now carry tags ("fellow coach: bayar makan …"), and the tracker applies them on send. The tracker's capture view already returns them, but the companion ignores the field: the owner cannot see on the phone which tag was attached or whether a new one was created, and has to open Tracker Web to check.

## Expected Outcome
After a typed capture is recorded, the phone's result card and the capture's Rincian list the tags the tracker attached ("Tag: fellowship coach"). A capture without tags looks exactly as it does today, and editing a capture on the phone never removes its tags.

## What Changes
- The capture view's transaction reads `tags: [{id, name}]`; a tracker or a stored capture without the field means no tags.
- The recorded result card shows one muted line with a tag icon, "Tag: fellowship coach, Malang trip", below the recorded facts and the note.
- Rincian's "Yang dicatat tracker" gains a "Tag" row when the transaction has tags.
- Editing a capture on the phone keeps sending no tag fields, so the tracker keeps the tags; the view refreshed after the edit shows them.
- Release `android-v2.4.5` after the merge.

## Non-goals
- Adding, removing or creating tags on the phone. Tags are changed in Tracker Web's edit dialog.
- Tags in the Aktivitas feed rows, on notification-recorded transactions, or on moves (moves carry no tags).
- Any tracker (backend or web) change.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `mobile-text-capture`: a recorded capture shows its tags on the result card and in Rincian.

No project capability is added. `api-consumers` applies: the companion reads a new optional response field and must tolerate its absence.

## Impact
- `mobile/app/src/main/.../data/network/model/CaptureModels.kt`: `CaptureTag`, `CaptureTransaction.tags`.
- `ui/capture/CaptureResults.kt` (result card), `ui/capture/CaptureDetailScreen.kt` (Rincian).
- Tests: `CaptureModelTest` (parsing, old payload), the capture UI tests, and the edit request's JSON.
- No database migration: the capture view is stored as JSON (`summary_json`), and older rows simply have no tags.
- Local verification only until the owner accepts; the release tag is pushed after the merge and green Android checks.
