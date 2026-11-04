## Why

On 2026-11-04 one BCA → GoPay Tabungan top-up produced three notifications. Jago's "You've received Rp500.000 from GoPay." reported money GoPay had already reported. `recognise-one-arrival-seen-twice` added a deterministic guard that books such a second report as a duplicate observation and lists it in Tinjau under "Terlihat dua kali". It also gave the model time context (neighbours) and a `same_money_as` field.

The paid benchmark on 2026-11-04 showed the model never used `same_money_as`: 0% recall on the incident case, at both reasoning levels. It also added about 9% input tokens per notification. The owner then decided:

> "I don't think we need AI to see this, it's more about controling the notifications because there's redundant notification received for this case"

> "ok, after first confirmation but for now add it manually for known patter on my account case, create the plan"

So the duplicate is a notification-control problem with a deterministic answer. Today every repeat still waits in Tinjau for the owner to confirm, even after they confirmed the same pattern before.

## Expected Outcome

- **One prompt per notification:** the model reads one notification at a time again (`notification-interpretation-12`). The deterministic one-arrival guard stays.
- **Learned patterns:** once the owner confirms "Benar, satu transaksi" for a second report, that pattern is learned. For example, Bank Jago income is redundant when GoPay has already reported the same amount. Later reports with that pattern are skipped automatically: they book nothing, ask nothing, and are listed under "Dilewati otomatis" with "Catat terpisah".
- **Pengaturan → Notifikasi ganda:** lists the owner's patterns, stops one with "Berhenti melewati", and adds one by hand. The owner uses this to add the known Bank Jago (uang masuk) / GoPay pattern now.
- **A genuine report still records:** a Jago income with no matching GoPay report records normally.

## Approval

The owner asked for this plan in the session on 2026-11-04 (quotes above). **Plan approval, 2026-11-04.** After a summary of the plan, the owner said "approved, defaults ok". OD122–OD128 take their recommended defaults (design.md, open decisions), including the live benchmark of `-12` against `-11` (about $0.10).

## What Changes

- **Backend, AI context:**
  - remove the neighbour context, the `same_money_as` output field, its validation code `invalid_neighbour_reference`, the model-only hold `possible_duplicate_observation`, and the setting `NOTIFICATION_NEIGHBOUR_WINDOW_SECONDS`;
  - the prompt becomes `notification-interpretation-12`.
- **Backend, patterns:**
  - a new table `notification_skip_patterns` (migration V46) and a pattern match inside the one-arrival guard;
  - learning on "Benar, satu transaksi";
  - endpoints to list, add and stop patterns;
  - the Tinjau queue gains `skipped`, the "Dilewati otomatis" list.
- **Migration V46:** requeues events still held with `possible_duplicate_observation`, so the deterministic path decides them.
- **Frontend:**
  - Tinjau loses the held variant of "Terlihat dua kali" and gains the "Dilewati otomatis" fold;
  - Pengaturan gains the section "Notifikasi ganda" at `/settings/notifikasi-ganda`.
- **Evaluation:**
  - the corpus becomes `synthetic-notifications-5`;
  - the sequence cases now expect a plain observed leg for every event;
  - the `same_money` metric is removed.

## Non-goals

- **Skipping by text or sender:** a pattern always needs a recorded report of the same amount from the kept app. No notification is dropped without one.
- **Deleting owner memory entries:** OD121 stays as it is.
- **The Android companion:** no app or API change. A skipped event's result is its kept leg's result, as for any duplicate observation today.
- **Seeding the owner's pattern:** no migration or seed adds the Jago/GoPay pattern. The owner adds it through Pengaturan after the deploy, because the repository is public and production data changes only through the app.

## Capabilities

- **Modified:**
  - `ai-notification-processing`: neighbours and the same-money proposal removed, the benchmark sequence rescored;
  - `movement-pairing`: redundant-notification patterns;
  - `review-queue`: Seen-Twice section without holds, plus "Dilewati otomatis";
  - `currency-conversion-and-settings`: a ninth settings section.
- **No new project capability.** Existing standards apply: database (versioned additive migration), money (no ledger effect without a recorded kept report), multi-user (owner isolation) and llm (prompt version, offline and live benchmark).

## Impact

- **Backend:**
  - `services/notification_context.py` (`neighbour_context` removed);
  - `services/notification_interpretation.py` (`same_money_as`, validation);
  - `services/prompts/notification_interpretation.md`;
  - `services/notification_application.py` (`hold_possible_duplicate` removed, pattern lookup in `record_observed_leg`);
  - `services/movement_pairing.py` (`same_arrival`, `repair_duplicate_arrivals`);
  - `services/seen_twice.py`;
  - new `services/skip_patterns.py`;
  - `routers/review.py`, `routers/ingest.py`, `core/config.py`;
  - `docker-compose.yml`, `.env.example`.
- **Database:** `db/migrations/V46__notification_skip_patterns.sql`.
- **Frontend:**
  - `components/review/SeenTwiceCard.tsx`, `components/review/triage/TriageList.tsx`, `TriageInbox.tsx`;
  - `lib/review.ts`, `lib/triage.ts`, `lib/api.ts`, `lib/queryKeys.ts`;
  - `components/settings/sections.ts`, a new `components/settings/SkipPatterns.tsx`, a new route `app/settings/notifikasi-ganda/`.
- **Evaluation:**
  - `backend/evaluation/notification_cases.py` and `notification_benchmark.py`;
  - tests `test_notification_neighbours.py` (removed), `test_duplicate_arrivals.py`, `test_notification_benchmark.py`, `test_notification_cases.py`, and new pattern tests.
- **API:**
  - additive: new `skipped` and the skip-pattern endpoints;
  - `seen_twice` items lose `kind: "held"`.
