# Recognise one arrival seen by two apps

## Why

On 2026-10-31 at 18:39 WIB the owner topped up "GoPay Tabungan by Jago" from BCA. One real transfer produced three notifications within 3.4 seconds:

| Post time | App | Text | Recorded as |
| --- | --- | --- | --- |
| 18:39:18.228 | GoPay | "Anda telah menerima Tabungan by Jago sebanyak Rp500.000." | income, Gopay, interpretation remote = Jago · Main Pocket |
| 18:39:18.356 | Jago | "You've received Rp500.000 from GoPay." | income, Jago · Main Pocket (the default pocket), remote = Gopay; reasons `low_confidence`, `purpose_question`, `ambiguous_movement` |
| 18:39:21.665 | myBCA | "You spent IDR 500,000.00 at Payment." | expense, BCA · ATM; reasons `low_confidence`, `ambiguous_movement`, `awaiting_counterpart` |

At 18:41:48 a fourth notification followed, "You've moved Rp500.000 from GoPay Tabungan Pocket to your Tabungan Pocket". It was recorded correctly as its own movement.

**What happened.**
- Automatic pairing linked nothing. The BCA debit had two compatible incomes and was flagged ambiguous, which is correct under "Evidence Strength and Uniqueness".
- At 18:41:13 the owner answered the Jago row's purpose question as Internal Movement.
- At 18:41:23 the owner linked BCA → Gopay by hand (movement `b91109d4`).
- The Jago row stayed an unlinked income that was marked reviewed. Main Pocket carried a phantom Rp 500.000 until the owner deleted the row on 2026-11-04.

The read-only trace is in design.md, "Evidence".

**Contracts violated:**
1. **Ingestion books one arrival twice.** Two same-amount incomes, seconds apart, each naming the other's account as their remote, are one arrival seen by two apps. `record_observed_leg` (`backend/app/services/notification_application.py`) books both. Nothing in ingestion or pairing compares a new leg with a same-direction leg.
2. **A purpose answer of Internal Movement hides a missing side.** Answering a purpose question with an internal-movement category marks an unlinked leg `reviewed`, and never asks for its other side. So the phantom left no trace in Tinjau.
3. **Memory learns from transfers.**
   - The answer also upserted the owner memory `signature:jago|income|unknown → Internal Movement`. "Movement legs SHALL write feedback but no memory" was meant to prevent this. The row was not a movement leg when answered, but the answer described a transfer.
   - The AI memory `merchant:tabungan by jago → Pendapatan Lain` stayed after the owner linked that GoPay leg into a movement. A future unpaired GoPay "Tabungan by Jago" credit would then count as income.
4. **The interpreter is blind to time.** The model sees same-amount recorded legs within 900 s as `candidates`. It doesn't see neighbouring notifications that are not recorded yet, which app sent them, or how they are ordered. So it can't tell that a BCA debit, a GoPay credit and a Jago "received from GoPay" seconds apart are one transfer.

The owner, after confirming the flow and deleting the phantom row: "for the AI also must make it aware of time".

## Expected Outcome

The owner can see these results:

- **One transfer is one movement.** When one transfer is seen by several apps, the ledger records it once:
  - a second observation of the same arrival books no money;
  - it is attached to the leg it duplicates;
  - it is listed in Tinjau under "Terlihat dua kali", where the owner can confirm it, or record it separately if it really was a second transfer.
- **A transfer answer shows its missing side.** Answering a purpose question with "Internal Movement" either links the row to its other side, or keeps it in Tinjau as "Menunggu rekening pasangan", with its route shown as "? → Jago · Main Pocket".
- **Memory doesn't learn from transfers.** A transfer answer or a linked movement no longer teaches memory that files future notifications of that shape as income or as an unpaired movement.
- **The interpreter sees time.** It receives the owner's neighbouring notifications and legs within ±5 minutes, and it can propose that a notification is the same money as a neighbour. The deterministic rule stays the guard, whatever the model says.
- **A read-only report lists past cases.** It lists other past cases of the same pattern for the owner to review in the app. The case from 2026-10-31 is already repaired: the owner deleted row `d032e50b` on 2026-11-04, and Main Pocket is Rp 0, verified read-only.

## Approval

**Plan approval, 2026-11-04.** After a summary of both plans (`show-movement-routes-and-centered-dialogs` and `recognise-one-arrival-seen-twice`), the owner said "approved, defaults ok". OD116–OD120 take their recommended defaults (design.md, open decisions).

## What Changes

- **Deterministic guard: "same arrival".** Before pairing, a new observed leg SHALL be compared with recorded observed legs from the same owner with all of these properties (design D1):
  - same direction and amount;
  - a different institution app;
  - post times at most 120 s apart;
  - different accounts, where each leg's remote account (or its root) is the other leg's account (or its root).

  On a unique match:
  - the new event is recorded with provenance `duplicate_observation`, pointing at the kept leg, and creates no transaction;
  - the kept leg is the one whose account was not a default-pocket fallback, otherwise the first recorded one (design D2).
  - **Zero or several matches:** the current behaviour applies, and several matches add `ambiguous_movement`.
- **Tinjau "Terlihat dua kali".** `GET /api/review/queue` gains a `seen_twice` section of duplicate observations from the last 30 days.
  - "Benar, satu transaksi" acknowledges an item.
  - "Catat terpisah" records the event as its own leg through the existing manual resolution, with undo.
  - Items count in `unreviewed`, not in `attention` (OD117).
- **Purpose answers that name a transfer.** An answer whose category is an internal-movement category SHALL try to pair the row at once, by the same rules. Without a unique counterpart, the row stays `needs_review` with `awaiting_counterpart`, and is listed under `awaiting`.
- **Memory for transfers.**
  - A purpose answer or correction to an internal-movement category SHALL write feedback but no memory.
  - Linking a leg into a movement, by pairing, owner merge or completion, SHALL delete an `ai` memory entry for that leg's match key whose category is not a movement category.
  - Owner entries are never deleted automatically. The report lists owner entries in movement categories for the owner to remove.
- **Time-aware interpretation.**
  - The context gains `neighbours`: up to 20 of the owner's notification events and recorded legs with post times within ±5 minutes (`NOTIFICATION_NEIGHBOUR_WINDOW_SECONDS`, default 300). Each neighbour has a context-local id, app institution, direction, amount, signed seconds from this event, state (`queued`, `recorded` or `needs_review`), its account if recorded, and whether it is part of a movement.
  - The output schema gains `same_money_as: <neighbour id> | null`. The prompt version moves to `notification-interpretation-11`.
  - A `same_money_as` that the deterministic guard confirms is applied. One it does not confirm holds the event as `needs_review` with `possible_duplicate_observation` and books nothing (OD116).
  - When the neighbour query fails or exceeds the budget, the context says `neighbours_incomplete: true` and the model SHALL NOT propose `same_money_as`. The deterministic guard still runs at apply time.
- **Evaluation.** The synthetic corpus gains a sequence case built from this incident: BCA debit, GoPay credit, Jago "received from GoPay", then the Jago pocket hop two minutes later. It uses fictional names, and the fixture version moves to `synthetic-notifications-4`. The benchmark scores `same_money_as` precision and recall, and false duplicates as a safety failure.
- **Detector for past cases.** `scripts/verify/find_duplicate_arrivals.py`, read-only. It lists, for one owner:
  - unlinked recorded legs matching the guard's pattern;
  - recorded events whose transaction was deleted;
  - owner memory entries in movement categories.

  It prints dates, accounts, amounts and ids, and no notification text. Any repair happens in the app, by the owner.

## Non-goals

- No automatic rewrite or deletion of existing ledger rows.
- The 2026-10-31 case is already repaired by the owner.
- No change to how complete Jago pocket movements are recorded.
- No change to the Android companion.
- Change A (`show-movement-routes-and-centered-dialogs`) owns the route display. This change uses its "? → …" form only where Tinjau shows the awaiting row.
- **Local verification only.** Production is touched only by the approved deploy after the owner merges. The detector runs against production read-only, and only when the owner asks.

## Capabilities

**New capabilities:** none.

**Modified capabilities:**
- `movement-pairing`:
  - ADDED "One Arrival Seen by Two Apps";
  - ADDED "Linking Retires Automatic Memory".
- `ai-notification-processing`:
  - ADDED "Neighbouring Notifications in Context";
  - ADDED "Same-Money Proposal";
  - MODIFIED "Synthetic OpenAI Configuration Benchmark" (sequence cases).
- `notification-purpose-questions`: ADDED "Transfer Answers Ask for the Other Side".
- `merchant-memory`: ADDED "Transfers Teach No Memory".
- `review-queue`: ADDED "Seen-Twice Section".

**Project capabilities:** no new project capability. These apply:
- **llm:** prompt and schema versioned, eval case added, failure behaviour defined;
- **money:** no double booking, no silent drops;
- **background-jobs:** the deferred pass follows the same guard;
- **personal-data:** neighbours are sanitized like the rest of the context, and the detector prints no notification text.

## Impact

- **Backend:**
  - `services/notification_application.py`: guard in `record_observed_leg`, `duplicate_observation` provenance;
  - `services/movement_pairing.py`: shared guard, retire AI memory on link;
  - `services/notification_context.py`: neighbours;
  - `services/notification_interpretation.py` and `prompts/notification_interpretation.md`: schema, prompt and version;
  - `services/purpose_questions.py`: transfer answers;
  - `services/merchant_memory.py`;
  - `routers/review.py`: `seen_twice`, acknowledge;
  - `core/config.py`: the window setting.
  - **Migration `V45__duplicate_observation_review.sql`:** adds the nullable `notification_events.reviewed_at` for the "Benar, satu transaksi" acknowledgement (design D6). `provenance_kind` is an unconstrained `VARCHAR(30)`, so the new value needs no schema change.
- **Frontend:** Tinjau's "Terlihat dua kali" section and actions, plus labels for `possible_duplicate_observation`.
- **Evaluation:** `backend/evaluation/notification_cases.py` and `notification_benchmark.py`.
- **Scripts:** `scripts/verify/find_duplicate_arrivals.py`.
- **Tests:** the regression for the incident sequence, which fails before the fix, plus the guard's negative cases, purpose answers, memory, context, review queue and the benchmark.
