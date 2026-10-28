## Approval

Owner authorized phases 2–8 on 2026-10-28 without per-phase review; coordinator-reviewed.

## Why

Phase 3 of `research/notes/2026-10-28-automation-quality-plan.md`. The production evidence pass found
that the ingestion pipeline does not learn and names most rows generically:

- myBCA "Financial Diary" notifications never contain the merchant, only BCA's own category
  ("You spent IDR … at Groceries.", "… at Account Transfer.", "… at Payment."). The model therefore
  writes generic names such as "Belanja" or "Pembayaran", and the owner renames them later from memory.
  Most owner corrections were rows of this kind.
- Incoming masked transfers ("You received IDR … from ***AB ****CD *E") were mostly friends repaying
  shared meals, which the owner decided are reimbursements, not income.
- Food rows were split between need and want with no rule behind the split.
- `merchant_category_rules` is empty and nothing writes it: corrections are thrown away.
- When the category, the model's answer or a movement pairing is uncertain, the event is held out of
  the ledger, although its amount, direction and account are proven. Settled money then misses
  reports until the owner opens the phone review screen.
- The model's confidence is stored (phase 2) but never used.

## Expected Outcome

- Every settled notification whose amount, currency, direction and owned account are proven gets a
  ledger row. When only its category, name, pillar or movement pairing is uncertain, the row is
  recorded with `review_status = needs_review` and the reasons are kept with the event.
- Each owner correction of a row's merchant, notes, category, pillar, type or offset is kept as an
  append-only labelled example and teaches a per-owner merchant memory. The next notification from a
  merchant the owner has classified is recorded from memory, without a model call.
- The model sees the owner's memory and the most similar past transactions with their final labels,
  is told not to use placeholder names when a merchant is known, and rows it classifies below the
  confidence threshold (default 0.8) are flagged for review.
- Every recorded expense or income whose notification names no merchant or sender gets an "Apa ini?"
  question with up to five ranked suggestions. The owner answers with one tap or a short text, from
  the companion (version 1.8.0 or later) or the web, and the answer updates the row, marks it reviewed
  and teaches memory.

## What Changes

- Database (V29 plus its identical startup mirror): `merchant_memory`, append-only
  `classification_feedback` (an update trigger rejects changes), and an index for open purpose
  questions.
- Backend ingestion:
  - record-and-flag for uncertain category (uncategorised row), model fallback, low model confidence,
    model review reasons, ambiguous pairing and open purpose questions; holding stays for uncertain
    money facts and owned-account confirmations;
  - classification order: owner merchant memory, then the model with richer context, then the
    deterministic path;
  - `merchant` filled on notification rows; placeholder counterparties are no longer used as names;
  - model context: owner memory and similar history instead of the last 20 rows, trimmed to the
    existing 60 KB cap; prompt version `notification-interpretation-9` with a `merchant` field;
  - confidence gate `NOTIFICATION_REVIEW_CONFIDENCE_THRESHOLD` (default 0.8).
- Learning: feedback rows and memory upserts from every owner edit hook of phase 2 (transaction
  edit, movement rename, merge, top-up conversion, top-up notes) and from purpose answers.
- Purpose questions: creation and ranked suggestions, `POST
  /api/ingest/notifications/{event_id}/answer-purpose`, `GET /api/ingest/purpose-questions`, a
  strictly schematised free-text normaliser with its own prompt, and companion version gating
  (1.8.0) of the new result fields.

## Non-goals

- Companion UI for the question (phase 4b implements against the contract in `design.md`).
- Web review queue UI and bulk actions (phase 6); this change only exposes the open questions.
- Fee-tolerant or deferred movement pairing (phase 5).
- Re-classifying history or backfilling memory from past rows (phase 7).
- Statistical habit suggestions beyond the ranking defined here (phase 9).
- Removing `merchant_category_rules` or the startup mirror (phase 8).
- Production authority. Verification is local: a disposable PostgreSQL migrated V1→V29, the full
  backend suite with mocked providers, and OpenSpec validation. Deployment happens only through the
  GitHub workflow after the owner merges.

## Capabilities

### New Capabilities

- `notification-record-and-flag`: the boundary between recording with a review flag and holding an
  event, the review reasons, and the confidence gate.
- `merchant-memory`: match keys, the memory and feedback stores, learning from owner edits, the
  classification order and the model context built from memory and similar history.
- `notification-purpose-questions`: the "Apa ini?" question, suggestion ranking, the answer and
  list endpoints, the free-text normaliser and companion version gating.

### Modified Capabilities

- `transaction-provenance`: notification rows may now be recorded as `needs_review`, and memory hits
  are classified by `rule`.

The project capability list is unchanged (database, money, personal-data, multi-user, llm,
api-consumers apply). Standards: migration safety with rollback, owner scoping of every reference,
no notification text, names, amounts or model output in logs, bounded and redacted model input, and
mocked providers in tests.

## Impact

- **Migrations:** `db/migrations/V29__merchant_memory_and_purpose_questions.sql` and
  `backend/app/db/merchant_memory_and_purpose_questions.sql`, run last by `init_db.py`.
- **Backend:** new `services/merchant_memory.py`, `services/purpose_questions.py`,
  `services/prompts/purpose_normalization.md`; changes to `notification_application.py`,
  `notification_processing.py`, `notification_context.py`, `notification_interpretation.py`,
  `openai_notification_provider.py`, `prompts/notification_interpretation.md`, `provenance.py`,
  `core/config.py`, routers `ingest.py`, `transactions.py`, `movements.py`, services
  `movement_linkage.py`, `investment_topups.py`, the benchmark fixtures; tests.
- **API:** additive. New endpoints above. Notification results gain `review_status` and `question`
  for companions 1.8.0+. Fewer events end in `needs_review`; more rows carry `review_status =
  needs_review`.
- **Data:** two new tables. No existing row changes.
- **Cost:** no new paid service. The free-text normaliser is one small model call per typed answer;
  memory hits remove model calls for known merchants.
