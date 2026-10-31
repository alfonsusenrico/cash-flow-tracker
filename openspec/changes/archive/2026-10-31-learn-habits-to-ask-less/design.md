# Design

## Context

Phase 3 made every owner answer and correction an append-only labelled example in `classification_feedback`, with local weekday and hour, amount, account, institution, text signature and match key. Merchant memory turns an exact merchant key into an automatic classification, but signature keys (merchant-less payments such as myBCA "You spent IDR x at Groceries.") never short-circuit, so each of those payments gets "Apa ini?". The phase 9 vision (research note `2026-10-28-automation-quality-plan.md`) planned a statistical "Sepertinya …?" learner over those examples. The owner approved it on 2026-10-31 with automatic recording above an approved threshold, which supersedes the earlier mitigation "never auto-record a suggestion without a tap".

A read-only aggregate count of production on 2026-10-31 found one week of labels: 12 "Apa ini?" answers across 7 categories (none above 33%) and 17 memory rows. Habits will therefore start slowly, and the thresholds must hold up on small samples.

The learner is deterministic statistics. No model is called, prompted or changed, so the provider evaluation baseline is unaffected. The `genai-engineering` rules applied are: a confidence gate before a consequential action, an offline evaluation with recorded results, recording what produced each action, content-free logs and a kill switch.

## Decisions

### D1. Evidence is the owner's own labels

A feedback row counts as evidence only when it is the owner's own classification:

- every `correction` (written only by owner edits through the transaction, movement, merge, top-up and bulk-review paths);
- a `purpose_answer` whose result was `reviewed`, meaning a chosen suggestion or a classified text answer; an answer whose text the normaliser could not classify keeps the earlier automatic category and is excluded;
- a `capture` whose recorded row was classified by the owner.

Excluded: accepted history suggestions (bulk acceptance of model or rule proposals, see open decision 2), rows recorded from a habit (they write no feedback), AI or rule rows (they write no feedback), and other owners' rows (the evidence query is filtered by owner, and the category join also requires the owner's category).

Only the latest feedback row of each transaction counts. A later correction replaces the answer, and a later non-owner row (for example an accepted suggestion) removes the transaction from the evidence.

V39 adds `before_classified_by`, `after_classified_by` and `after_review_status` to feedback rows so these rules are exact for new rows. Rows written before V39 have NULL there, and an update would break the append-only trigger, so they are classified at read time. A legacy purpose answer counts when its event's `interpretation.purpose_answer` holds the same `reply_id` with `review_status = reviewed`. A legacy capture never counts (conservative, and captures carry no bank signature anyway). Legacy corrections count.

Rejected:
- Using the transaction's current `classified_by` or `review_status`. Habit rows are `reviewed` without being the owner's, and rows change after the label was written.
- Counting reviewed transactions instead of feedback. That would include habit records and bulk "Terima" acceptances of AI rows, which is self-reinforcement.

### D2. Habit key

The key is the institution, the bank category signature (`text_signature`, for example `bca|expense|groceries`) and the observed account. The amount is not a bucket but a neighbourhood around the current payment (D3).

Time of day and weekday are not in the key. They break ties between equally weighted classifications and choose the wording (notes and merchant) among the agreeing labels: the label that best fits the current hour (±2 h) and weekday, then the newest. They never change the agreement, so they cannot push a payment over the threshold.

Typed captures carry no institution or signature, so they never match a notification key. They are eligible evidence by D1 but contribute nothing today; merchant memory already covers repeated captures.

Rejected: adding hour or weekday buckets to the key. With one week of real labels every key would split into near-empty cells, and the approved threshold of three would almost never be reached.

### D3. Amount band: ±25% as a ratio

A label is in the band of a payment when each amount is at most 1.25 times the other (`4·a ≤ 5·b` and `4·b ≤ 5·a`, integer arithmetic in bigint). The band is symmetric in log space, has no fixed boundaries and scales with the amount.

Simulation evidence (`research/evidence/2026-10-31-habit-simulation.md`, 200 seeds, eight weeks): the ±25% ratio gave the lowest wrong-automatic rate, 9% of automatic records in both scenarios. ±50% gave 10–11% and fixed log2 buckets 11–12%, with the same or a higher question rate. Wider bands merge the 22k snack and 45k lunch habits, and log2 buckets split a habit that straddles a power of two (for example 60k ± 30% across 65 536).

Rejected: ±50%, log2 buckets (the existing ranking's `amount_band`), exact amounts (rarely repeat).

### D4. Thresholds and weighting

The approved thresholds are unchanged. A payment is recorded automatically when at least 3 labelled payments exist and the leading category and pillar hold at least 90% of the weight. The comparison is done in integers, so 9 of 10 passes exactly. Category and pillar must both agree.

An answer weighs 1 and a correction 2. The window holds at most 10 labelled payments (D5). With one disagreeing correction in the window the best possible agreement is 9/(9+2) ≈ 82%, so one correction always makes the next payment ask, which is what the owner asked for. With weight 1, nine agreeing labels would absorb a correction (9/10 = 90%).

Simulation: correction ×1 asks less (65% against 69% overall; habitual lunch 25% against 36–41% from week 2 on) with about the same wrong rate (8% against 9% of automatic records in the steady scenario, 10% against 9% under drift). ×2 is kept because it guarantees the re-ask; see open decision 1.

Rejected: a cached habit table updated on each answer. Stateless evaluation from feedback re-evaluates at once and cannot drift from the evidence.

### D5. Evidence window

Only the latest 10 labelled payments of the key count. Without a window, a changed habit is never learned again: under drift the habitual lunch stayed asked 100% of the time after week 5, against 6–9% with the window by weeks 7–8. Older evidence ages out by count, not by calendar time, so a monthly habit keeps its evidence for months.

Rejected: time decay. It loses rare but steady habits and adds a parameter without evidence.

### D6. Eligibility (never automatic)

A habit is evaluated only for a candidate expense with a merchant-less key whose observed account resolves deterministically. It is not evaluated for:

- income;
- an own-account or RDN transfer;
- a payment naming the owner;
- a self-identity match;
- unresolved account names or a mapping error;
- a transfer-shaped text (the existing `transfer_signature` pattern: transfer, kirim, top up, pindah …).

At recording time the habit applies only where a purpose question would otherwise be attached. That rules out a counterpart pairing, a self-transfer and the movement category. It also requires the habit's account, a non-ambiguous pairing, and a category that is still an active expense category of the owner. Labels in non-reportable categories ("Internal Movement", "Investasi") are not evidence.

Only labels change: category, pillar, notes and merchant. Amount, date and account come from the notification as before.

### D7. Recording from a habit

The habit decision is computed in `load_context` (`_habit`, kept out of the model input like other underscore fields). When it records automatically, processing treats it like an owner memory hit and does not call the model. The deterministic interpretation supplies the facts. In `record_observed_leg` the habit then replaces the classification and its doubts: `classified_by = habit`, `review_status = reviewed`, no confidence (only a model's confidence is stored), no review reasons and no purpose question. The event's interpretation stores `source = habit` and the basis `{category_id, kakeibo, support, agreeing, agreement}`.

A habit record writes no feedback and no memory, so it never becomes evidence. Ingestion stays idempotent: the payload hash and the `notification:<event id>` key are unchanged, and a resent payload returns the stored result.

A habit-recorded leg that is later paired as a movement gets `classified_by = system`, because the pairing classifies it from then on.

Rows recorded from a habit are `reviewed`, so they are not in Tinjau, the attention count or history-suggestion runs (eligible classifiers stay `rule` and `ai`). The model's similar-history context and capture resolution no longer treat habit rows as owner-confirmed, so habits do not feed back into the model.

### D8. "Sepertinya <kategori>?" first

Below the threshold (or with the kill switch off), the habit's guess is the first suggestion: kind `habit`, label "Sepertinya <kategori>?", the guessed notes (or the category name), merchant, category and pillar, and `basis {agreeing, support}`. The identical history group is not repeated. Tapping it is a purpose answer like any other, so it becomes evidence.

### D9. Weekly question rate

`GET /api/dashboard/question-rate?weeks=1..26` (default 8, otherwise 422) counts the owner's `notification` and `capture` expenses and incomes that are not movement legs or fees, by the local ISO week of the transaction date in `TZ`, oldest first, with the current week `in_progress`. Each row has exactly one outcome:

- `asked`: the notification interpretation holds the `purpose_question` reason, or the owner resolved the event manually, or the capture needed answers. It counts as `answered` when a purpose answer exists, the row is now owner-classified, or the capture was answered.
- Otherwise by interpretation source: `memory`, `habit` or `ai`. A capture recorded by `rule` counts as memory and by `ai` as AI.
- `other`: deterministic rules and legacy rows.

Corrections are `correction` feedback rows of notification or capture transactions, counted in the week they were made; `habit_corrections` are those whose previous classifier was `habit`. `has_data` is false when nothing was recorded or corrected. The read runs in a read-only repeatable-read transaction.

The Analisis "Bandingkan & Kakeibo" tab shows the panel "Seberapa sering ditanya" below the recurring charges. Each week, newest first, shows "Ditanya n dari m (p%)", a bar (decorative, `aria-hidden`; the text carries the value), the shares by Memori, Kebiasaan, AI, Aturan & lainnya and Dijawab Anda, and the corrections. Weeks without rows say "Tidak ada transaksi tercatat.", there is an honest empty state, and a failed load shows no figures.

### D10. Explanation

- The notification result for companions 1.8.0 and later adds `habit: {label, basis}` when the row is still habit-classified.
- Ledger list and detail add `habit_basis {agreeing, support}`, read from the event only for habit rows.
- The web ledger row shows "Dicatat dari kebiasaan · n dari m"; the edit dialog shows "Dicatat dari kebiasaan: <kategori> · n dari m jawaban sebelumnya" with a hint that changing it makes the next similar payment ask again; the classifier label is "Kebiasaan".

Android, read-only check of `financial-tracker-mobile-listener` main at `d33b39a`:
- Gson ignores the unknown `habit` field.
- `classifiedBy` is parsed only for capture transactions, which are never habit-recorded.
- Suggestion `kind` is never used for display, and a habit suggestion's id (`s1`) and label (at most 160 characters) pass `PurposeQuestions.inspect`.
- A habit-recorded result is `reviewed`, which the phone treats as "no open question".

Nothing breaks or is mislabelled. Follow-up: show `habit.label` and `habit.basis` on the phone row.

### D11. Kill switch

`HABIT_AUTO_RECORD_ENABLED` (default true) is read at evaluation time. False means no automatic record, while the guess stays first. It is passed through `docker-compose.yml` and documented in `.env.example`.

## Data, API and failure behaviour

- V39 is additive and repeatable:
  - it replaces `chk_transactions_classified_by` to allow `habit`;
  - it adds three nullable feedback columns;
  - it adds two indexes: `(user_id, account_id, text_signature)` for key lookup and `(user_id, transaction_id, created_at DESC)` for latest-per-transaction.
- Evidence query: at most 10 rows after filtering, once per notification evaluation during context loading. An integer-overflow bug (psycopg sends small Python ints as `smallint`, and `5 · 12 000` overflowed) was found by the existing suite and fixed with `bigint` casts. The Rp 12.000 tests cover it.
- If the habit's category was archived between evaluation and recording, `recordable_habit` refuses it and the payment is asked as before. If a category changes kind concurrently, the existing category lock raises `invalid_current_category` and the event goes to review, the existing failure path.
- Logs: no new log lines; nothing logs notes, amounts or labels.

## Migration and rollback

- Forward: Flyway V39 on deploy; no backfill and no row rewritten.
- Rollback of code only: the old code never writes `habit`. Existing habit rows stay valid under the V39 constraint. The old web shows the raw classifier "habit" (unknown values are shown as sent), the old backend leaves habit rows alone (history suggestions take only `rule` and `ai`) and ignores the new feedback columns. Setting `HABIT_AUTO_RECORD_ENABLED=false` is the faster rollback of behaviour without a deploy.
- Rolling back the schema is not planned. Dropping the columns would lose labelled metadata, and restoring the old constraint requires first changing habit rows to `rule`, which an explicit owner-approved migration would have to do.

## Evaluation

`scripts/verify/habit_simulation.py` is a deterministic offline simulation: synthetic, no database, 200 seeds and eight weeks of 14 merchant-less payments a week. The payers are:

- a weekday lunch (95% one category);
- a weekly shop (100%);
- an ambiguous small shop (55/45);
- an afternoon snack (85/15);
- three unrelated payments.

The drift scenario changes the lunch category from week 5. The owner answers every question truthfully and corrects a wrong automatic record with probability 0.9. Results are in `research/evidence/2026-10-31-habit-simulation.md`, and `test_offline_simulation_asks_less_each_week_with_few_wrong_records` guards the chosen configuration.

Chosen configuration, compared with the current behaviour (100% asked):
- Steady: weekly question rate 89% → 64% by week 8; habitual lunch 69% in week 1, 35–41% from week 2.
- Drift: lunch re-asked at 80–86% in weeks 5–6, then 6–9% by weeks 7–8.
- Wrong automatic records: 9% of automatic records, 3% of all payments, mostly from the 85/15 snack habit, where three agreeing answers are common (0.85³ ≈ 61%). Raising the minimum support to 5 changed this only to 8–9% (2.1% of payments) while asking 4 points more, so the approved minimum of 3 is kept.

The overall rate falls less than the habitual payer's because the ambiguous and one-off payments correctly stay asked.

## Open decisions for owner

1. Correction weight. ×2 (implemented) guarantees that one correction re-asks. ×1 would ask about 4 points less overall (habitual lunch about 25% instead of about 38%) at a similar wrong rate, but nine agreeing answers would absorb a correction.
2. Accepted history suggestions are not habit evidence (conservative: they are bulk acceptances of model or rule proposals). Counting them would start habits sooner from the owner's history-fix runs.
3. Android display of the habit label (follow-up in the companion repository).
4. The Tinjau page shows the "Sepertinya …?" chip like other suggestions, without its basis. Showing "2 dari 2" there is a small follow-up if the owner wants it.
