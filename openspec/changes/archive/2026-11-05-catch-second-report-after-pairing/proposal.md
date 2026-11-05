## Why

Without an account link, the one-arrival guard catches a second report of the same money only while the first report is still unpaired. Whether that holds depends on which notification the server processes last.

- **4 Nov 2026, 20.25 (Rp1.100.000):** the order was GoPay, Jago, myBCA. Jago's "You've received … from GoPay" came in while the GoPay credit was still unpaired, so it was held under "Terlihat dua kali".
- **5 Nov 2026, 07.34 (Rp10.000):** the order was GoPay, myBCA, Jago. The GoPay credit had already paired with the myBCA debit into "BCA → GoPay". The guard's query skips legs that are in a movement (`SAME_ARRIVAL_SQL`, `t.movement_id IS NULL`), so Jago's identical text became a new income in Jago · Main Pocket and added Rp10.000 to its balance. The owner deleted it by hand.

The two cases have the same texts, the same accounts and less than 2 seconds between reports. Only the processing order differed. A new owner with no links yet gets an inflated balance whenever the bank's report arrives before the second app's. The same order also hides the very item whose answer teaches the account link.

Tinjau's list row for a "Belum dikenali" question always shows "−" and "Pembayaran tanpa nama toko", even for an income. On 5 Nov the row read "−Rp 10.000" while the item said "+Rp10.000 masuk".

## Expected Outcome

- **Any order:** the second report of one arrival is held under "Terlihat dua kali" in any of the three orders, also when the first report has already paired into a movement. The movement stays as it is, and no balance changes.
- **Paired legs stay:** a leg that is already in a movement is never removed or replaced by a later report.
- **Real transfers:** when an unpaired opposite-direction leg of the same amount sits on the first report's account within 120 seconds, the guard steps aside and pairing decides, so a real follow-up transfer is not swallowed. This is the same safeguard that linked reports already use.
- **List row:** a "Belum dikenali" row shows "+" with the income colour for an income and "−" for an expense. Its caption reads "Uang masuk" for an income and keeps "Pembayaran tanpa nama toko" for an expense.

## Approval

On 2026-11-05 the owner said "ok create the plan and do fix", after the diagnosis of the 07.34 case and the proposed two-part fix.

## What Changes

- **Guard query:** `movement_pairing.SAME_ARRIVAL_SQL` also returns legs that are already in a movement, and it returns their `movement_id`.
- **Never replace a paired leg:** `replaceable_leg` is false for a leg in a movement. This covers `keeps_new_observation`, the link swap and the deferred pass.
- **Possible transfer:** `same_arrival` hands the leg to pairing (returns no decision) when an unpaired opposite-direction leg of the same amount is on the original's account within `SAME_ARRIVAL_WINDOW_SECONDS`.
- **Deferred pass:** a pair whose original is already in a movement is decided from the unpaired leg, whichever of the two was posted first.
- **Tinjau row:** `TriageList` signs and colours a question's amount, and picks its caption, from the transaction's direction. `itemTitle` falls back to "Uang masuk" for an income with no merchant or notes.

## Non-goals

- No change to account links, their window or their learning.
- No change to the window (120 seconds) or to the naming rule (each leg names the other's account).
- No repair of past bookings. The 5 Nov Rp10.000 was already deleted by the owner.
- No migration and no Android companion change.
- No production action from the agent beyond read-only checks. The release goes through the merge and the deploy workflow.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `movement-pairing`: "One Arrival Seen by Two Apps" covers a first report already in a movement, the possible-transfer safeguard and the deferred pass.
- `review-queue`: a new requirement covers how a "Belum dikenali" row shows its direction.

No project capability is added. The money, database and multi-user standards apply through the existing tests on a migrated disposable PostgreSQL.

## Impact

- **Backend:** `backend/app/services/movement_pairing.py`, with tests in `backend/tests/test_duplicate_arrivals.py`.
- **Frontend:** `frontend/src/components/review/triage/TriageList.tsx` and `frontend/src/lib/triage.ts`, with tests in `frontend/src/app/review/page.test.tsx`.
- **API and data:** no API, schema or data change. The Tinjau list already receives `transaction.type`.
