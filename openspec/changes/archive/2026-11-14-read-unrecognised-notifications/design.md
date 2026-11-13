## Context

Today's notification pipeline has three stages:

1. Fixed rules (`collect_facts` with the parser) prove the amount, direction and settlement.
2. The model (`Interpretation`) classifies the event and proposes accounts.
3. Backend checks guard the money: verbatim quotes, owned references, the mapping threshold, the source-pocket question and pairing.

The model reads only what stage 1 accepted. Wording outside the fixed lists either lands in `needs_review` (when a direction cue exists) or is ignored without being stored.

Production evidence, read-only on 2026-11-13: five Jago "You've added Rp X into Y Locked Pocket." events, all `settlement_unproven`. Four were entered by hand as moves into Bayar Kredit, 43 minutes to 14 hours before the notification. The source question already exists for unnamed Jago outbound transfers (companion 1.6.0+; the owner runs 2.4.1).

## Goals / Non-goals

**Goals:**
- Unrecognised wording is read by the model.
- Money facts stay proven by quotes and backend checks.
- Unknown pocket sources are always confirmed by the owner, helped by a suggestion.
- Nothing is recorded twice.

**Non-goals:**
- Replacing the rules for known formats.
- Suggestions for unnamed outbound transfers.
- Matching hand entries for fully named moves.

## Decisions

### 1. Unrecognised wording becomes a model-read candidate

`collect_facts` keeps every noise, promotion, headline, foreign-currency, injection and amount check. When the only remaining blocker is unproven settlement, it returns `status = "candidate"` with `needs_reading = True` instead of `settlement_unproven`. This covers both today's `needs_review` case (direction cue present) and today's silent ignore (no direction cue). The direction may stay `None`.

Facts with `needs_reading` and no stored reading are never recorded by code: the deterministic path, memory and habits raise `settlement_unproven`. In deterministic mode (model off or misconfigured) the event keeps today's `needs_review` / `settlement_unproven`.

Rejected: adding "you've added" to the phrase list. That is the hard-coding the owner asked to stop.

### 2. The reading is one model answer, checked and stored

The existing interpretation call is reused. The context gets `facts.needs_reading = true`, and the prompt gives these rules for that case:

- Decide whether money has already moved. Promotions, reminders, pending or failed items, and balance or limit information are `ignored`.
- For `record`, give the direction, and quote in `direction_evidence` the words that state the completed activity.
- For a pocket move, quote the pocket names exactly as written in `source_evidence` / `target_evidence`. Leave the source quote empty when the text names no source.
- Choose account IDs for the named pockets with a `mappings` item each. For an unnamed source, choose the most likely pocket from history as a suggestion.
- Lower the confidence when unsure.

The backend accepts a reading only when all of these hold:

- outcome `record`;
- the amount quote is one of the facts' amount quotes;
- `direction_evidence` and any endpoint quote appear verbatim in the sanitized text;
- a direction cue found by the rules agrees with the model's direction;
- confidence ≥ `notification_review_confidence_threshold`;
- a `internal_movement` reading comes from Jago.

An accepted reading is stored as `model_reading`:

- `direction`, `source_name`, `target_name`, `source_named`;
- `suggested_source_account_id`;
- `confidence`, `prompt_version`;
- `text_hash` (of the sanitized text).

A reading that fails these checks leaves the event in `needs_review` with code `wording_unconfirmed`, and nothing is recorded. An `ignored` answer ignores the event as today.

`collect_facts` applies a stored reading whose `text_hash` matches. The direction comes from the reading, the parser's pocket names are replaced by the quoted names, and `needs_reading` becomes false. Every later step therefore sees the same facts without another call: the source answer, mapping confirmation, recording and pairing. "Coba Proses Ulang" clears the reading, so the event is read afresh.

Rejected:
- A second, extraction-only model call. It costs more and has a new schema.
- Keeping the reading only in memory. The source answer arrives later, and a second reading could differ.

### 3. Pocket names use the existing mapping

The quoted names go through `pocket_endpoint` as the parser's names do. An unknown name such as "Bayar Kredit Locked" becomes an `unresolved_name`. The model's mapping for that role is accepted at `notification_mapping_auto_threshold` and learned as an alias, or else asked as today. Because the reading was stored first, a mapping question survives the round trip.

Rejected: dropping "Locked" in code. It is one more fixed rule, and the mapping already learns it after the first event.

### 4. The parser stops guessing the main pocket for other verbs

`JAGO_IN_POCKET_PATTERN` and `JAGO_OUT_POCKET_PATTERN` require their move verb ("moved", "has been moved", "memindahkan", "telah dipindahkan"). Today's "You've moved Rp X into your Y Pocket" still means main → Y. Other "into … Pocket" wording no longer becomes a main-pocket move; it reaches the model as unrecognised.

### 5. Source question for a model-read move

`requires_source_pocket` is also true for candidate Jago facts with direction `internal_movement` and a reading with `source_named = false`. The question is reused, with these differences:

- the options are every owned liquid account that holds its own balance, at any institution, except the target. An account that only groups pockets is left out. Money into a pocket may come from another pocket or from another bank or wallet, so the list is not limited to Jago;
- the question and the options response carry `suggested_account_id`, kept only if it is an option.

Recording depends on where the answered source is:

- **Same institution as the target:** one bilateral movement, as today.
- **Another institution:** the arrival is recorded as the inbound leg and completed on that account with Tinjau's completion (`complete_movement`).
  - A single unlinked matching leg in the movement category is linked.
  - Without one, the outbound leg is created with the completion key, so that account's own notification attaches to it later.
  - Several candidates leave the arrival in review (`ambiguous_movement`) for the owner to choose.

Rejected: a fixed "from outside Jago" answer. The owner asked for no answers shaped around one person's habits. Listing the owner's own accounts covers any user's banks and wallets.

The answer becomes the move's source in `deterministic_interpretation`, which now uses `confirmed_source_account_id` for a move. After the answer, the event is processed in deterministic mode from the stored reading, so no new model call is made. No answer, a stale answer or an invalid answer never falls back to the main pocket.

### 6. Hand-entered moves are confirmed, not duplicated

This applies to moves with an unnamed source. Candidates are hand-entered movements into the target account that meet all of these:

- same amount;
- dated within 48 hours of the notification;
- the inbound leg's `idempotency_key` is not `notification:%`;
- no notification event already references the movement.

What happens:

- **Before the question:** exactly one candidate means the event is recorded as `role_confirmation` with role `inbound` and nothing is written. Any other count means the question is asked.
- **After the answer:** candidates are narrowed to that source. One means confirm. None means create the movement. Several mean `needs_review` with `ambiguous_existing_movement`.

48 hours covers the observed 14-hour gap. A wrong match links a move with the same pocket and amount that the owner already made, so balances stay right.

### 7. Companion

The source-pocket controls read `suggested_account_id` and preselect that option. They label it "Disarankan" and leave the confirm button to the owner. Older builds ignore the field and show no preselection, as today.

## Data, API and failure behaviour

- Migration V56: `ALTER TABLE notification_events ADD COLUMN model_reading JSONB`. It is nullable and needs no backfill. Rollback is safe: older code ignores the column.
- The API is additive: `suggested_account_id` appears in the question and the options response.
- Provider failure or an invalid answer is handled as today: retry, then `needs_review`. A reading never comes from the deterministic fallback.

## Risks

- **More model calls:** one per unrecognised notification that isn't noise. The benchmark measures this before release, and the owner accepted the cost.
- **A wrong reading:** the quote and confidence checks, the mapping threshold, the owner's source answer and the duplicate check limit it. A wrong direction with a high-confidence quote would still record. The benchmark's noise and pending cases measure this risk.
- **Prompt version bump:** existing behaviour for recognised events must not change. The existing benchmark cases are rerun in the same paid run.
