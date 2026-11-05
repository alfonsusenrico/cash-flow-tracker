## Context

`same_arrival` (`backend/app/services/movement_pairing.py`) decides whether a new observed leg repeats one already recorded. With an account link it uses `LINKED_REPORT_SQL`, which already accepts a kept leg in a movement ("a late Jago report still repeats a GoPay credit that paired with its BCA debit meanwhile"). Without a link it uses `SAME_ARRIVAL_SQL`, which filters `t.movement_id IS NULL`. The filter dates from `recognise-one-arrival-seen-twice`, where the guard could also remove the recorded leg (`keeps_new_observation`), and removing a leg in a movement would break the movement.

These are production facts from read-only checks on 2026-11-05. The tests restate them with synthetic data.

| Post time (UTC) | App | Text | Processed |
| --- | --- | --- | --- |
| 00:34:06.189 | GoPay | "Anda telah menerima Tabungan by Jago sebanyak Rp10.000." | 00:34:10.98, income on GoPay, remote Jago · Main Pocket |
| 00:34:07.318 | myBCA | "You spent IDR 10,000.00 at Payment." | 00:34:14.40, paired with the GoPay credit into BCA · ATM → GoPay |
| 00:34:07.521 | Jago | "You've received Rp10.000 from GoPay. …" | 00:34:16.73, income on Main Pocket, remote GoPay, purpose question |

Every guard condition held for the Jago leg: another app, same direction and amount, 1.3 seconds apart, different accounts, each naming the other's account root. Only the movement filter excluded the GoPay credit. On 4 Nov, with the same texts, Jago was processed before myBCA and was held.

## Goals / Non-Goals

**Goals:**
- Hold the second report in any processing order.
- Never remove or replace a leg in a movement.
- Keep a real follow-up transfer from being swallowed now that more legs can match.
- Show a question row's direction truthfully.

**Non-Goals:**
- Changing the window, the naming rule, account links or their learning.
- Repairing past bookings.
- Migrating any data.

## Decisions

### D1. The guard also matches a leg already in a movement

`SAME_ARRIVAL_SQL` drops `t.movement_id IS NULL` and selects `t.movement_id`. All other conditions are unchanged.

Rejected alternatives:
- **Delay pairing until the guard window has passed.** The owner rejected holds on 2026-11-04: "preserve transaction order is better then waiting like that".
- **Re-check after pairing and unpair the movement.** This undoes a correct movement to fix a later report, and it adds churn and undo complexity.
- **Rely on account links only.** A new user has no link, and the item that teaches the link is the one that goes missing.

### D2. A leg in a movement is never replaceable

`replaceable_leg` returns false when `movement_id` is set. This one predicate guards every swap path: `keeps_new_observation` in the live path, the link swap and both branches of the deferred pass. The link path's explicit `row["movement_id"] is None` check becomes redundant and is removed.

Rejected alternative: checking `movement_id` at each call site. Four call sites would each need the check, and a future one would miss it.

### D3. A possible transfer hands the leg to pairing

Once `same_arrival` has its single close original, it runs `POSSIBLE_TRANSFER_SQL` on the original's account:
- an unpaired leg in the other direction;
- the same amount;
- within `SAME_ARRIVAL_WINDOW_SECONDS` of the new leg.

If that leg exists, the guard returns no decision, and the normal pairing decides. Example: GoPay credited, then GoPay "udah dikirim ke Bank Jago" for the same amount, then Jago "received from GoPay". The Jago credit is the real other half of the GoPay debit.

This applies whether or not the original is in a movement. Widening D1 makes such coincidences likelier, and the unpaired case had the same gap. Linked reports have used this safeguard since `reconcile-linked-account-reports` D2.

Rejected alternative: applying it only to paired originals. An unpaired original followed by a real transfer would keep the same false-duplicate risk.

### D4. The deferred pass decides a paired pair from its unpaired leg

The pass walks only unpaired legs and decides a pair from its later observation. A paired original posted after the unpaired leg would never be visited, so its pair would never be decided. The pass therefore skips `original.post_time > current.post_time` only when the original is unpaired. With D2 the unpaired leg is always the one dropped, and the pass stays idempotent.

### D5. A question row follows the transaction's direction

`itemAmount` uses `transaction.type`: "+" with `ws-amt-in` for an income, "−" for an expense, as group rows do. `itemLine` reads "Belum dikenali · Uang masuk" for an income and keeps "Belum dikenali · Pembayaran tanpa nama toko" for an expense. `itemTitle` falls back to "Uang masuk" for an income with no merchant or notes.

The label is in neutral standard Indonesian (UI label register). Rejected alternative: "Uang masuk tanpa nama pengirim". It is false when the sender is named, as GoPay was on 5 Nov.

## Data, API and failure behaviour

- **Data:** no schema or API change. The duplicate observation stores the kept leg's `result_key` and `result_snapshot`. A paired leg's snapshot is already the movement's, so the companion result shows the movement.
- **Failure:** failures behave as before. One more read query runs inside the existing transaction and fails the same way.
- **Rollback:** revert the release. Events held under the new rule stay valid duplicate observations, which an older release lists and acts on.

## Risks / Trade-offs

- **[A real second credit of the same amount, from another app, naming each other, within 120 seconds of a paired leg, is held.]** The owner sees it in "Terlihat dua kali" and chooses "Catat terpisah", which can be undone. D3 covers the common real-transfer shape.
- **[A link learned from a paired kept leg.]** `learn_from_acknowledgement` reads the kept leg's account, which pairing does not change. The 4 Nov acknowledgement linked GoPay to the Jago app as expected.
