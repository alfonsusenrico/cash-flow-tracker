## Context

See `proposal.md`. State on `main` after phase 2 (422ef71):

- `notification_application.deterministic_interpretation` raises `uncertain_category_mapping` when
  no merchant rule or bank-category hint selects exactly one category; the event is held.
- `notification_processing.apply_claim` sends a model `needs_review` answer or an unusable answer to
  `apply_deterministic_fallback`, which records with `review_status = auto`.
- `record_observed_leg` records an unpaired leg when several counterparts are compatible and only
  sets `error_code = 'ambiguous_movement'` on the event.
- `load_context` sends the model categories (with `default_kakeibo`), `merchant_category_rules`
  (always empty) and the last 20 transactions of any kind.
- The model's `confidence` is stored on AI rows and otherwise unused.
- Phase 2 tracks owner edits (`OWNER_EDIT_ASSIGNMENTS`) in the transaction PATCH/PUT, movement
  PATCH, merge, top-up conversion and top-up notes edit, but keeps nothing of the previous values.
- The sender question (V25) is the pattern for owner questions: `notification_events.active_question`
  JSON with a `question_id`, receipts unique per `reply_id`, companion version gating on the event's
  `source_version`.
- Parser placeholders: `BCA Payment` (myBCA outgoing), `BCA` (myBCA incoming without sender),
  `QRIS Merchant`, `Debit`, `Bank Jago` and `Bank Jago Payment` (Jago payment without merchant).

## Goals / Non-Goals

**Goals:** settled money always reaches the ledger with an honest flag; owner corrections are kept
and reused; the model gets the owner's labels; merchant-less rows are explained by the owner with as
little typing as possible, through a contract the companion can implement unchanged.

**Non-Goals:** see the proposal.

## Decisions

### D1. Record-and-flag boundary

A notification is recorded when its facts are `candidate` (one IDR amount, settlement and direction
proven, timestamp with zone, no conflicting companion hints) and its owned account resolves without
an owner confirmation. Within that boundary these doubts flag the row instead of holding it:

| Reason | When |
|---|---|
| `uncertain_category` | the deterministic path finds no single category; the row is recorded with `category_id = NULL` and no pillar |
| `model_fallback` | AI mode, and the row came from the deterministic fallback after the model answered `needs_review` or gave no usable answer |
| `low_confidence` | AI row with confidence below `NOTIFICATION_REVIEW_CONFIDENCE_THRESHOLD` |
| `model_flagged` | AI row with `review_reason` other than `none` |
| `ambiguous_movement` | unpaired leg with more than one compatible counterpart, or candidate overflow |
| `purpose_question` | a purpose question was attached (D10) |

Flagged rows get `review_status = needs_review`; the reasons are stored in the event's
`interpretation.review_reasons`. Owner resolutions stay `reviewed`.

Still held (no row): every `needs_review` fact code (`ambiguous_amount`, `missing_amount`,
`settlement_unproven`, `unsupported_currency`, `conflicting_mobile_*`, `fractional_amount`,
`timestamp_timezone_required`, `unsafe_instructions`, `message_too_large`); account mapping errors and
mapping confirmation; source-pocket confirmation; sender confirmation; `ambiguous_existing_movement`
and `movement_role_consumed` (a second report of a movement would double the money);
`incomplete_context`; invalid current references; `conflicting_source_pocket`. A model `ignored`
answer stays final (open decision 1).

Automatic pairing turns both legs into a movement in "Internal Movement", so the doubts about the
leg's category no longer apply: both legs get `review_status = auto` unless the owner classified one,
and an open purpose question on the counterpart event is closed.

*Rejected:* recording uncertain-category rows in a guessed category ("Lainnya"). It hides the doubt
inside a real category; an uncategorised row is visibly unfinished, and phase 6 adds the
"Tanpa kategori" bucket. *Rejected:* flagging model rows by `review_reason` only. The model is asked
to calibrate confidence; the gate makes that number useful.

### D2. Match keys

`services/merchant_memory.py`:

- `notification_merchant(facts, identity)` returns the parsed counterparty unless it is empty, a
  placeholder (`BCA Payment`, `BCA`, `QRIS Merchant`, `Debit`, `Bank Jago`, `Bank Jago Payment`,
  `Transaksi bank`), masked
  (contains `*`), the owner's name, an owned-account transfer (`own_account_transfer`, RDN) or a
  movement.
- `text_signature(facts)` = `institution|direction|kind[|from:<mask>]`, where `kind` is the normalised
  bank category hint, `qris` or `debit` for those texts without merchant, or `payment`; the mask part
  is present only for a masked sender.
- Match key: `merchant:<normalised merchant>` when a merchant exists, else `signature:<signature>`.
  Normalisation: NFKC, casefold, whitespace collapsed, at most 200 characters.
- A manual row with a merchant uses `merchant:<normalised merchant>`.

Recorded events store `match_key`, `text_signature` and `merchant_less` in `interpretation`, so
later edits and the similar-history query do not re-parse the text; for older events the key is
recomputed from the stored text.

### D3. Tables (V29)

```sql
CREATE TABLE merchant_memory (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  match_key VARCHAR(220) NOT NULL,
  key_kind VARCHAR(10) NOT NULL CHECK (key_kind IN ('merchant', 'signature')),
  display_merchant VARCHAR(120) NULL,
  notes VARCHAR(500) NULL,
  category_id UUID NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
  kakeibo_type VARCHAR(20) NULL CHECK (pillar set or NULL),
  offset_category_id UUID NULL REFERENCES categories(id) ON DELETE SET NULL,
  source VARCHAR(10) NOT NULL CHECK (source IN ('owner', 'ai')),
  hit_count INTEGER NOT NULL DEFAULT 0 CHECK (hit_count >= 0),
  last_used_at TIMESTAMPTZ NULL,
  created_at, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, match_key)
);

CREATE TABLE classification_feedback (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  kind VARCHAR(20) NOT NULL CHECK (kind IN ('correction', 'purpose_answer')),
  transaction_id UUID NULL, event_id UUID NULL,          -- no FK: rows outlive what they describe
  question_id UUID NULL, reply_id UUID NULL, suggestion_id VARCHAR(16) NULL, answer_text VARCHAR(200) NULL,
  before_merchant, before_notes, before_category_id, before_category_name, before_kakeibo_type,
  before_type, before_offset_category_id,
  after_merchant, after_notes, after_category_id, after_category_name, after_kakeibo_type,
  after_type, after_offset_category_id,
  local_weekday SMALLINT (1 = Monday … 7 = Sunday), local_hour SMALLINT (0–23),
  amount BIGINT, account_id UUID, institution VARCHAR(30), text_signature VARCHAR(200),
  match_key VARCHAR(220), transaction_source VARCHAR(20),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (user_id, reply_id),
  CHECK (kind <> 'purpose_answer' OR (question_id IS NOT NULL AND reply_id IS NOT NULL))
);
-- BEFORE UPDATE trigger raising 'classification_feedback is append-only'.
CREATE INDEX ... classification_feedback (user_id, after_type, created_at DESC);
CREATE INDEX ... notification_events (user_id, post_time DESC) WHERE active_question->>'type' = 'purpose';
```

Feedback has no foreign keys to transactions, events or categories: an `ON DELETE SET NULL` would
update rows in place, which the append-only rule forbids, and phase 9 needs the example after the
row or category is gone. Category names are copied for the same reason. Deletion of the user still
cascades. The pillar stored is the effective pillar (own pillar, else the category's).

*Rejected:* evolving `merchant_category_rules`. Its `merchant_pattern` is a substring rule with only
a category; reusing it would change the meaning of a live table that `deterministic_interpretation`
reads. It stays empty and phase 8 removes it. *Rejected:* a separate receipts table for purpose
answers. The feedback row is the receipt (`UNIQUE (user_id, reply_id)`), so an answer and its
labelled example cannot diverge.

### D4. Writing feedback and memory

`record_owner_corrections(cur, user_id, before)` takes snapshots read before an owner edit, reads
the rows after it, and for each row whose merchant, notes, category, effective pillar, type or offset
changed inserts a `correction` feedback row and, for a plain expense or income (no movement) with a
category and a match key, upserts memory with source `owner`. It also closes an open purpose question
on the row's event. Hooks: transaction PATCH/PUT when `classification_change` is true, movement PATCH
when notes change, merge, top-up conversion and top-up notes edit. Review-only PATCHes write nothing.

Owner upsert overwrites every value and sets `source = 'owner'`, keeping `hit_count`. An AI upsert
(`ON CONFLICT … WHERE merchant_memory.source = 'ai'`) happens only for AI rows that passed the gate
with a merchant key and a category.

### D5. Classification order

1. Owner memory hit: exact `merchant:` key, `source = 'owner'`, category active with the
   notification's direction, offset (if any) an active expense category. Recorded with interpretation
   source `memory` → provenance `notification / auto / rule`, merchant and notes from memory, hit
   count and `last_used_at` updated. In AI mode `process_claim` skips the provider; `apply_claim`
   re-checks the hit under the event lock.
2. AI mode: the model with the D7 context; then the gate (D8).
3. Deterministic path (AI disabled, or model fallback): merchant rules, bank hint, else
   uncategorised (D1).

Signature keys never short-circuit: "at Groceries" covers many shops, so a memory hit there would
repeat the last answer for every grocery payment.

### D6. Merchant on notification rows

`transactions.merchant` gets, in order: the memory display merchant, the model's `merchant` (a new
nullable Interpretation field), or the notification merchant (D2), each normalised and cut to 120
characters. Deterministic rows without merchant get a generic Indonesian description instead of a
placeholder: "Pengeluaran" or "Pemasukan", followed by the bank category when present.

### D7. Model context

- `memory`: up to 30 entries; exact key first, then owner before AI, then hit count and recency.
  Fields: `match_key`, `merchant`, `description`, `category_id`, `kakeibo`, `source`.
- `similar_history` replaces `history`: up to `NOTIFICATION_AI_HISTORY_LIMIT` (default 20) expense or
  income rows of the same direction, excluding movement legs and archived accounts, matching either
  the same `match_key`/merchant or the same institution with amount between half and double. Order:
  key matches first, then owner-classified or reviewed, then newest. Fields: `match`
  (`same_key`/`same_institution_amount`), `merchant`, `description`, `category_id`, `kakeibo`
  (effective), `amount`, `date`, `owner_confirmed`.
- `classification`: `match_key`, `merchant_less`.
- Budget: the serialised context must fit 60 000 bytes (`CONTEXT_BYTE_LIMIT`, shared with the
  provider). History is dropped from the end, then memory from the end. If the rest is still too big
  the provider keeps returning `incomplete_context` as before.
- Redaction: merchant and descriptions go through `sanitize_text` with the owner's aliases.

### D8. Prompt and gate

`PROMPT_VERSION = "notification-interpretation-9"`. Additions: use `memory` and `similar_history`
(owner-confirmed rows are the strongest evidence); never use placeholders such as "Pembayaran",
"Belanja", "Transaksi bank" when the notification names a merchant or a memory entry matches; return
`merchant` (the merchant's display name, or null); when `classification.merchant_less` is true,
propose the most likely purpose from similar history with confidence below 0.8, and otherwise a
neutral description; never invent a merchant name.

`NOTIFICATION_REVIEW_CONFIDENCE_THRESHOLD` defaults to 0.8 and must be within 0.5–1.0; an invalid
value sets `processor_configuration_invalid` like the other numeric settings.

### D9. Purpose question creation

Attached in `record_observed_leg` after the insert when: no counterpart was linked, the direction is
expense or income, the match key is a signature key, the row is not a self or owned-account transfer,
and the event is not an owner resolution. The question is stored in `active_question` with a fresh
`question_id`. Prompt text, Indonesian, IDR with Indonesian separators:

- expense: `Apa ini? Rp187.500 dari <account label>` + ` · kategori bank: <hint>` when present;
- income: `Apa ini? Rp57.000 masuk ke <account label>` + ` dari <sender name or mask>`.

### D10. Suggestion ranking

Examples: the owner's last 300 feedback rows with `after_type` equal to the direction and an active
`after_category_id`. Score per example:

| Feature | Points |
|---|---|
| same full `text_signature` | 4 |
| else same signature without the `|from:` part | 2 |
| same account | 2 |
| same amount band `floor(log2(amount))` | 2; adjacent band 1 |
| local hour within 2 hours (circular) | 1 |
| same local weekday | 1 |

Examples scoring 0 are ignored. Examples with the same (merchant, notes, category, pillar, offset),
compared case-insensitively, form one suggestion whose score is the sum; ties go to the newest
example. Then the current classification (`kind = current`) when the row has a category and it is
not already listed, then the repayment suggestion (`kind = repayment`, the owner's active
`offsets_spending` income category, notes "Ganti uang patungan", or "Ganti uang dari <name>" when the
sender is named) for income from a transfer. At most five; the repayment suggestion replaces the last
one when needed. Suggestion ids are `s1`…`s5` in order.

### D11. Answer endpoint

`POST /api/ingest/notifications/{event_id}/answer-purpose`, session or Bearer auth.

```json
{"question_id": "<uuid>", "reply_id": "<uuid>", "suggestion_id": "s1"}
{"question_id": "<uuid>", "reply_id": "<uuid>", "text": "sewa lapangan arena futsal"}
```

Order:
1. Read the event as owner (404 otherwise). A feedback row with the same `reply_id` is a replay:
   same event, question, suggestion and text → 200 with the current result; else 409
   `purpose_answer_conflict`.
2. 409 `stale_question` unless the open question is `purpose` with that `question_id`; 409
   `purpose_not_applicable` when the transaction is gone or became a movement leg; 422
   `unknown_suggestion` for an id not in the question.
3. Text answers: one normaliser call outside the database transaction (D12).
4. Under the owner's notification lock: repeat step 1–2 checks, validate the target category (owned,
   active, matching kind; else 422 `invalid_suggestion` for suggestions), the pillar (expense only)
   and the offset (only for income in an offsetting category, an eligible expense category), update
   the transaction (`merchant`, `notes`, `category_id`, `kakeibo_type`, `offset_category_id`,
   `classified_by = owner`, `classification_confidence = NULL`, `owner_edited_at`, `review_status`),
   insert the `purpose_answer` feedback row, upsert memory for the event's key and, when a merchant is
   known, for its merchant key, close the question, and set the result description to the new notes.

Body validation (422 `invalid_purpose_answer`): exactly one of `suggestion_id` (1–16 characters) and
`text` (1–200 characters after trimming, no control characters).

Response: `{"ok": true, "result": <notification result, gated per D13>, "answer": {"status":
"applied" | "needs_review", "transaction_id", "merchant", "notes", "category_id", "category_name",
"kakeibo", "offset_category_id", "review_status"}}`.

### D12. Free-text normaliser

`OpenAINotificationProvider.normalize_purpose(context)` with its own prompt
`prompts/purpose_normalization.md` (`PURPOSE_PROMPT_VERSION = "purpose-normalization-1"`) and strict
schema:

```json
{"merchant": "string|null (≤120)", "description": "string (1–160)", "category_id": "uuid|null",
 "kakeibo": "need|want|culture|unexpected|saving|null", "offset_category_id": "uuid|null",
 "confidence": 0.0}
```

Input: the redacted answer text, direction, amount, account label, bank category hint, the owner's
categories of that direction with default pillars and `offsets_spending`, eligible offset categories
for income, and up to five ranked suggestions. The answer is validated: category of the right kind
and active, no pillar on income, offset only on an offsetting income category and from the eligible
list. Success with a category → `applied`, `reviewed`. Success without a category → the category
stays, `needs_review`. Provider unavailable (AI disabled or misconfigured), any `ProviderError`, or a
failed validation → notes = the owner's trimmed text, merchant and category unchanged,
`needs_review`. The event records `purpose_answer.normalizer` as `ok`, `no_category` or the error
code; logs carry only those codes.

### D13. Result contract and version gating

`PURPOSE_MIN_VERSION = (1, 8, 0)` against the event's `source_version` (a retry from a newer build
updates it, as today). For a recorded expense or income result, companions 1.8.0+ also get:

```json
{
  "payload_hash": "…", "event_id": "…", "status": "recorded",
  "record_key": "notification:…", "type": "expense", "description": "Pengeluaran Groceries",
  "source": "BCA Harian", "target": null, "amount": 187500, "currency": "IDR",
  "transaction_id": "…",
  "review_status": "needs_review",
  "question": {
    "type": "purpose",
    "question_id": "…",
    "transaction_id": "…",
    "direction": "expense",
    "amount": 187500,
    "currency": "IDR",
    "prompt": "Apa ini? Rp187.500 dari BCA Harian · kategori bank: Groceries",
    "allows_text": true,
    "text_max_length": 200,
    "suggestions": [
      {"id": "s1", "kind": "history", "label": "makan kafe senja", "merchant": "Kafe Senja",
       "notes": "makan kafe senja", "category_id": "…", "category_name": "Makanan & Minuman",
       "kakeibo": "want", "offset_category_id": null, "offset_category_name": null},
      {"id": "s2", "kind": "current", "label": "Pengeluaran Groceries", "merchant": null,
       "notes": "Pengeluaran Groceries", "category_id": null, "category_name": null,
       "kakeibo": null, "offset_category_id": null, "offset_category_name": null}
    ]
  }
}
```

`kind` is `history`, `current` or `repayment`; `label` is the text to show on the chip. `question` is
absent when no question is open; `review_status` is one of `auto`, `needs_review`, `reviewed`. Older
companions get the result without both fields. `transaction_id` keeps its existing rule. After an
answer the result has the new `description`, `review_status = reviewed` (or `needs_review` when the
normaliser failed) and no `question`.

### D14. Web listing

`GET /api/ingest/purpose-questions?limit=50` (1–200) returns
`{"ok": true, "questions": [{"event_id", "post_time", "question", "transaction": {"id", "type",
"amount", "date", "account_id", "account_name", "notes", "merchant", "category_id", "category_name",
"kakeibo_type", "review_status"}}]}`, newest first, owner-scoped, excluding deleted and movement
rows. Not version-gated.

### D15. V29 and the startup mirror

`V29__merchant_memory_and_purpose_questions.sql` uses `CREATE TABLE IF NOT EXISTS`,
`CREATE INDEX IF NOT EXISTS`, `CREATE OR REPLACE FUNCTION` and a guarded trigger creation, so the
identical mirror `backend/app/db/merchant_memory_and_purpose_questions.sql`, run last by `init_db.py`,
is a no-op after Flyway. No existing row changes.

## Risks / Trade-offs

- [More rows flagged `needs_review`] → Intended: the queue is honest. Phase 6 adds bulk review; the
  phone answers merchant-less rows one tap at a time.
- [Uncategorised spending] → Counts in total outflow, not in a category; the Kakeibo report treats a
  pillar-less, category-less expense as need (phase 1 rule). Phase 6 adds the bucket.
- [A wrong owner answer teaches memory] → Memory follows the newest owner answer, every answer stays
  in feedback, and a later correction overwrites the entry.
- [Merchant memory applies to an ambiguous merchant name] → Only exact keys apply; the owner can
  correct a row, which updates the entry.
- [Model latency on text answers] → One bounded call with the existing timeout; failures degrade to
  storing the text.
- [Rollback] → The previous release ignores both tables; events recorded with flags stay recorded,
  and their `active_question` JSON of type `purpose` is ignored by old result code because it only
  reads questions on `needs_confirmation` events.

## Migration Plan

1. Merge; the GitHub workflow deploys; Flyway applies V29; the mirror is a no-op.
2. Companion 1.8.0 (phase 4b) may be released after this backend is live.
3. Post-deploy checks (owner authority): both tables exist and are empty; a new myBCA payment is
   recorded with `needs_review` and an open purpose question; `prompt_version` of new AI events is
   `notification-interpretation-9`.
4. Rollback: redeploy the previous release. To remove V29 entirely: drop the trigger, the function,
   both tables and the partial index.

## Open decisions for owner

1. **Model `ignored` stays final** (audit B7). Recording a settled candidate the model calls noise
   would put real noise in the ledger; it needs its own evidence first.
2. **Uncertain category is recorded uncategorised**, not in a guessed category (D1).
3. **Signature memory never auto-classifies** (D5); every merchant-less payment is asked, as decided.
   If repeated identical answers should auto-apply later, phase 9 adds that with a confidence rule.
4. **Purpose questions on masked incoming transfers** are asked even when the sender is already
   named, because the purpose (repayment, gift, payment) is still unknown.
5. **Model fallback rows are flagged**, including provider failures that previously recorded as
   `auto`.
6. **The repayment suggestion carries no offset category**; the owner picks it on the web. History
   suggestions carry the offset the owner chose before.
7. **Paired movement legs return to `auto`** after automatic pairing (D1).
8. **Feedback keeps notes after the transaction is deleted**, as personal data in the owner's own
   database, because phase 9 needs the labelled example.
9. **Salary and other sender-less incoming texts get a purpose question** because they name no
   counterparty. If that proves noisy, a later change can exempt bank categories the owner marks as
   self-explanatory.
