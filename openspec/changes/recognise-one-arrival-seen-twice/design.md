# Design: recognise one arrival seen by two apps

## Context

The incident is in proposal.md. This change follows root-cause debugging: observations first, then the violated contracts and where they are owned, then the fix and a regression that fails before it.

## Evidence

Production was read on 2026-11-04 with a read-only session (`default_transaction_read_only=on`) on the owner's account. These are observations, with ids shortened.

| Leg | Account | Type | Created | Classified | Owner edit | Movement |
| --- | --- | --- | --- | --- | --- | --- |
| `a61ccfd9` GoPay credit | Gopay | income | 18:39:24.9 | owner | 18:41:23.8 | `b91109d4` inbound |
| `d032e50b` Jago credit | Jago · Main Pocket | income | 18:39:27.8 | owner | 18:41:13.4 | none |
| `7bd34a10` myBCA debit | BCA · ATM | expense | 18:39:30.5 | owner | 18:41:23.8 | `b91109d4` outbound |

- **Events, all processed by OpenAI on their first attempt:**
  - GoPay created at 18:39:19.0, recorded at 18:39:24.9;
  - Jago created at 18:39:19.9, recorded at 18:39:27.8;
  - myBCA created at 18:39:22.1, recorded at 18:39:30.5.

  So the Jago event was interpreted while the GoPay leg was already recorded, and the GoPay leg was in its `candidates`.
- **Interpretations:**
  - GoPay: `account_id` Gopay, `remote_account_id` Main Pocket, `remote_root_account_id` Jago, `transfer: true`.
  - Jago: `account_id` Main Pocket (the default pocket), `remote_account_id` Gopay, `transfer: false`, `match_key` `signature:jago|income|unknown`.
- **Classification feedback:**
  - 18:41:13, `purpose_answer`: the Jago row went from Pendapatan Lain to Internal Movement.
  - 18:41:23, two `correction` rows for the owner's merge of BCA and GoPay.
- **Memory:**
  - `signature:jago|income|unknown` → Internal Movement, source `owner`. It was created on 2026-10-30 by an earlier answer and updated at 18:41:13.
  - `merchant:tabungan by jago` → Pendapatan Lain, source `ai`, created on 2026-10-30. The merge didn't change it.
- **Today:** the owner deleted `d032e50b` on 2026-11-04, and Main Pocket reads Rp 0. Event `58f42cb8` stays `recorded` with `transaction_id` null, through the `ON DELETE SET NULL` foreign key.

**Root cause.** It is a conjunction of four conditions, each with its owner:
1. **Two observations booked.** `record_observed_leg` (`notification_application.py`) checks a new leg against legs of the opposite direction (`find_counterpart`), and against complete movements on the same account (`confirm_completed_movement`, the role-confirmation block). It never checks a same-direction leg on another account, so the Jago observation of the GoPay arrival became a second income. This is the first violated contract: one real money movement, one ledger effect.
2. **The pair became ambiguous.** The second income made the myBCA debit's counterpart ambiguous, so nothing paired automatically and the owner had to resolve it by hand.
3. **The transfer answer hid the missing side.** `apply_answer` (`purpose_questions.py`) applies an Internal Movement answer as `reviewed` without trying to pair. The leg left Tinjau with one side missing.
4. **Memory learned from a transfer.** `learn_from_owner` (`merchant_memory.py`) skips only rows that already have a `movement_id`, so the transfer answer taught `signature:jago|income|unknown`. Linking the GoPay leg left its AI memory "Pendapatan Lain" in place.

The model contributed: it had the GoPay leg as a candidate and still answered with a plain income. The prompt says that a single cross-bank notification records only its observed leg, and that candidate history is a hint. Both are right for movements, but give no rule for a second observation. The model alone can't be the fix, because ledger effects are the backend's (project LLM boundary).

## Goals

- One money movement, one ledger effect, whatever the order the apps report it in.
- Every hold or duplicate stays visible and reversible for the owner.
- The model gains time context without gaining authority.

## Non-goals

- Rewriting history automatically.
- Detecting duplicates between a notification and a typed capture. Captures have their own matching.

## Decisions

### D1. The guard

The guard runs in `record_observed_leg` after `confirm_completed_movement` and the role-confirmation block, before `find_counterpart`, and under the per-owner advisory lock already held there. It runs in `repair_owner` with the same function, `same_arrival(cur, user_id, leg)`, in `movement_pairing.py`.

A recorded leg `L` matches the new leg `N` when all of these hold:
- `L` is an `observed_leg` event's transaction: unlinked, not split by the owner, and with no fee link.
- `L.type == N.type` and `L.amount == N.amount`.
- The institutions differ (`facts.institution` of each event).
- `|L.post_time − N.post_time| ≤ 120 s` (OD118).
- `L.account_id != N.account_id`.
- `root(N.remote) == root(L.account)` and `root(L.remote) == root(N.account)`. Roots are used because a default-pocket fallback can land on a different pocket of the same bank.

Each of these gives a result:
- **Exactly one match:** a duplicate observation (D2).
- **Two or more:** record `N` as today and add `ambiguous_movement`.
- **None:** today's path.

**Rejected:**
- **Amount and time only.** Two real same-amount top-ups are common. The incident's own scenario "Weak pair with a repeated amount" shows the risk.
- **Matching against queued events.** The guard sees only committed legs. Each event is re-checked when its own turn comes, so order is covered by D2.

### D2. Which leg is kept

The leg whose account came from the notification itself is kept. The other leg, the one whose account came from the default-pocket fallback (`main_endpoint` in `notification_resolution.py`), is dropped. The resolution records `account_basis: "named" | "default_pocket"` in the interpretation signature. When both or neither came from the fallback, the first recorded leg is kept.

- **The later event is the one to drop:** it becomes `duplicate_observation` (`transaction_id` = the kept leg) and no transaction is written.
- **The already recorded leg is the one to drop:**
  - It is dropped only if it is still `auto` or `needs_review` with no owner edit, no movement and no open answer. Its transaction is then removed through `delete_transaction_rows` in the same database transaction, the new leg is recorded, and the old event becomes the `duplicate_observation` of the new leg.
  - Otherwise the new event becomes the duplicate (OD119).

Rejected: always keeping the first. In the incident's reverse order this would keep the default-pocket leg, and Main Pocket would hold money that went into GoPay Tabungan.

### D3. Duplicate observation and Tinjau

- A `duplicate_observation` event has `processing_state = 'recorded'` and `provenance_kind = 'duplicate_observation'`, and points at the kept leg through `transaction_id`. `discover_candidates` and the pairing candidates exclude it. Its compact result is the kept leg's result, so the companion shows one transaction.
- `GET /api/review/queue` returns `seen_twice`:
  - duplicate observations and `possible_duplicate_observation` holds from the last 30 days with `reviewed_at IS NULL`;
  - each item gives the app label, amount, post time and the kept leg's route.
- **Actions:**
  - `POST /api/review/seen-twice/{event_id}/acknowledge` sets `reviewed_at`.
  - "Catat terpisah" uses the existing manual-resolution path (`apply_manual_resolution`) on that event: it records the leg as observed and returns an undo token.
  - For a hold (D5), the path is the same.

### D4. Transfer answers

`apply_answer` runs `find_counterpart` for the row after applying an answer in an internal-movement category:
- **A unique counterpart:** `link_pair` runs in the same transaction, and the answer's undo restores both rows (existing movement undo).
- **None:** the row stays `needs_review` with `awaiting_counterpart`.

`learn_from_owner` returns early for a movement category. `link_pair` and the owner merge and completion paths delete the leg's `ai` memory entry for its match key when that entry's category is not a movement category.

### D5. Time context for the model

- `notification_context.neighbour_context(cur, user_id, facts)` returns up to 20 neighbours within `NOTIFICATION_NEIGHBOUR_WINDOW_SECONDS` (default 300, range 30–1800, invalid values fall back and report `processor_configuration_invalid`), nearest first.
- **Each neighbour:**
  - a context-local id (`n1`…`n20`), mapped by the backend to an event id;
  - institution, direction, amount;
  - `seconds_from_this` (signed);
  - state (`queued`, `recorded` or `needs_review`), and the account id if recorded;
  - `in_movement` (bool).
- The neighbours never include another event's text, so other merchants and masked names don't enter this event's prompt.
- **Budget:** neighbours are trimmed before `memory` and after `similar_history` in `fit_context_budget`. Trimming sets `neighbours_incomplete: true`.
- **Prompt:** `notification-interpretation-11` adds one paragraph:
  - A notification and a neighbour that report the same amount, direction and opposite owned accounts within seconds from two different apps are the same money: set `same_money_as` to that neighbour's id, and keep the other fields as for an observed leg.
  - A neighbour of the opposite direction is a possible movement counterpart, not the same money.
  - With `neighbours_incomplete`, `same_money_as` stays null.
- **Validation:** `validate_interpretation` maps the id back and rejects unknown ids as `invalid_neighbour_reference`.
- **Application:**
  - If the deterministic guard, run at apply time, finds the same leg, the event becomes a duplicate.
  - If the guard finds nothing, or another leg, the event is held as `needs_review` with `possible_duplicate_observation` and nothing is booked (OD116).
  - If the model says null but the guard matches, the guard wins.

The chain to a follow-up hop (GoPay Tabungan → Jago Tabungan two minutes later) needs no new rule. The hop is a complete Jago movement and is recorded by its own path. Its neighbours let the model describe it, and the earlier credit stays its source.

### D6. Migration

`V45__duplicate_observation_review.sql` adds `notification_events.reviewed_at TIMESTAMPTZ NULL`, plus a partial index on `(user_id, post_time) WHERE provenance_kind = 'duplicate_observation' AND reviewed_at IS NULL`.
- It is additive. An older release ignores the column.
- Rollback leaves the column unused.

### D7. Detector

`scripts/verify/find_duplicate_arrivals.py --user-id <uuid> [--since YYYY-MM-DD]` reads only, with `SET TRANSACTION READ ONLY`. It prints:
- pairs of unlinked recorded legs matching D1;
- recorded events with a null `transaction_id` and no movement;
- owner memory entries whose category is an internal-movement category.

It prints dates, account labels, amounts and ids, and no notification text. It is run against production only when the owner asks, through `docker exec … python -` with `PGOPTIONS=-c default_transaction_read_only=on`.

### D8. Verification gates

- **Regression, written first (fails on `main`):** a database test replays the incident sequence (GoPay, Jago 0.13 s later, myBCA 3.4 s later) with fictional names. It asserts:
  - one income on GoPay, no income on Kantong Utama, the Jago event `duplicate_observation`;
  - the myBCA debit paired with the GoPay credit;
  - balances.

  The same test in reverse order (Jago first) asserts the D2 swap.
- **Negative cases:** the guard's scenarios (30 s apart without mutual naming, the same app, 121 s, two originals, owner-edited earlier leg), plus the deferred pass and its idempotence.
- **Purpose answers:** transfer answer without and with a counterpart, and undo.
- **Memory:** no memory from transfer answers; AI entry retired on link; owner entry kept.
- **Context:** neighbours are bounded, sorted and sanitized; incomplete flag; another owner's events absent.
- **Model boundary:** an unknown `same_money_as` is rejected; an unconfirmed one is held; model off still guarded.
- **Benchmark:** the sequence case passes offline with the recorded expected outcomes. A live run of `notification-interpretation-11` is run only with the owner's approval, under the per-run dollar cap, and is reported as a model-quality result.
- **Review queue:** `seen_twice` counts and both actions; Tinjau browser check.
- **Money identities:** account balances, the overview and pulse figures are unchanged on a seed without duplicates.

## Compatibility, migration and rollback

- The migration is additive.
- The API adds a section and one endpoint.
- The prompt version is recorded per event, so behaviour is attributable.
- Rollback is a revert of the merge. Duplicate observations recorded meanwhile keep pointing at their kept legs, and nothing is lost.

## Risks

- **A real second transfer swallowed.** The guard needs mutual account naming within 120 s across two apps, and each swallowed event is listed in Tinjau with "Catat terpisah". The benchmark counts false duplicates as a safety failure.
- **The D2 swap deletes an automatic row.** It only happens while the row is untouched by the owner, and inside the same database transaction as the replacement.
- **Context size.** Twenty compact neighbours are about 2 kB, within the 60 kB budget.

## Open decisions (recommended defaults)

- **OD116: unconfirmed model proposal.** *Default:* hold `needs_review` and book nothing. The alternative is booking normally with a review flag.
- **OD117: counting.** *Default:* `seen_twice` items count in `unreviewed`, not `attention`. The alternative is counting them in `attention` too.
- **OD118: guard window.** *Default: 120 s.* The alternative is the role-confirmation window of 30 s.
- **OD119: earlier leg edited by the owner.** *Default:* the new event becomes the duplicate, even if its account was named. The alternative is holding both for the owner.
- **OD120: neighbour window.** *Default:* ±300 s, 20 entries. The alternative is ±900 s, the pairing window.
