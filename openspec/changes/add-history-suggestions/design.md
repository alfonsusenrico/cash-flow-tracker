## Context

See `proposal.md`. State on the base (0950cc6, phase 6 archived; merged into main as 7de01ac):

- Rows carry `source`, `review_status`, `classified_by`, `owner_edited_at` and `merchant` (V28). The
  V28 backfill set historic manual rows to `classified_by = owner`, other non-notification rows to
  `system`, and notification rows to `ai` or `rule`.
- `merchant_memory` and append-only `classification_feedback` (V29). Feedback `kind` is `correction`
  or `purpose_answer`; an update trigger rejects changes.
- Owner-edit hooks: `routers/transactions.classification_change`, `provenance.OWNER_EDIT_ASSIGNMENTS`,
  `merchant_memory.snapshot_rows` and `record_owner_corrections`; phase 6 `POST /api/review/bulk` uses
  them under the owner's notification advisory lock.
- The model provider (`OpenAINotificationProvider.respond`) takes a prompt, a context and a strict
  schema; `model_input` drops `_`-prefixed keys and enforces the 60 KB context limit; `sanitize_text`
  redacts the owner's names, contact data and secrets.
- `/review` (phase 6) shows questions, groups, awaiting and ambiguous movements; its empty state
  replaces the sections when nothing is left.

## Goals / Non-Goals

**Goals:** fix past automatic rows with proposals the owner decides on in groups; reuse the owner's
own memory before any model call; keep cost visible and bounded; make an accepted suggestion exactly
an owner edit; never re-ask what the owner rejected.

**Non-Goals:** see the proposal.

## Decisions

### D1. Tables (V31)

```sql
suggestion_runs (id, user_id → users ON DELETE CASCADE, from_date, to_date (≥ from_date),
  status running|completed|failed, max_rows, eligible_count, selected_count, processed_count,
  memory_count, rule_count, ai_count, unchanged_count, rejected_before_count, ai_calls,
  error_code, model, prompt_version, lease_token, lease_expires_at, created_at, updated_at, finished_at)
UNIQUE (user_id) WHERE status = 'running'            -- one active run per owner

transaction_suggestions (id, user_id → users CASCADE, transaction_id → transactions CASCADE,
  run_id → suggestion_runs SET NULL, proposal_source memory|ai|rule,
  proposed_merchant (120), proposed_notes (500), proposed_category_id → categories CASCADE,
  proposed_kakeibo_type (pillar or NULL), proposed_offset_category_id → categories CASCADE,
  confidence NUMERIC(4,3) 0..1, reason (160),
  before_type, before_merchant, before_notes, before_category_id, before_kakeibo_type (effective),
  before_offset_category_id,
  status pending|accepted|rejected|superseded, created_at, updated_at, decided_at)
UNIQUE (transaction_id) WHERE status = 'pending'     -- at most one pending suggestion per row

classification_feedback: + history_suggestion_id UUID NULL; kind CHECK widened to
  correction | purpose_answer | accepted_suggestion
```

Suggestions are proposals, not labelled examples: they may cascade with their row or category. The
labelled example of an acceptance is the feedback row, which has no foreign key and survives.
Widening the CHECK constraint updates no row, so the append-only trigger is not involved; it is
guarded so the startup mirror does not re-validate the constraint on every start.

*Rejected:* storing suggestions as JSON on the transaction (no history of rejections, no uniqueness of
pending suggestions); a feedback row for rejections (the row keeps its values, so a before/after
example would be empty; the rejected suggestion row is the durable record, see open decision 3).

### D2. Eligibility

```sql
t.type IN ('expense','income') AND t.classified_by IN ('rule','ai') AND t.owner_edited_at IS NULL
AND t.movement_id IS NULL AND t.fee_for_movement_id IS NULL AND t.goal_id IS NULL AND t.obligation_id IS NULL
AND no debt allocation AND category not "Internal Movement"/"Investasi"
AND t.date in [from_date 00:00, to_date + 1 day 00:00) in the configured time zone
```

newest first, `LIMIT HISTORY_SUGGESTION_MAX_ROWS`. The task statement was `classified_by <> 'owner'`;
this is narrower (open decision 1): `system` rows come from something the owner configured (a
recurring rule, payroll, a reconcile, a top-up) and NULL is not expected after V28. Goal- and
debt-linked rows are left out because their category drives debt and goal screens.

### D3. Estimate and start

`POST /api/review/suggestions/runs {from_date, to_date, confirm?}` (session or Bearer auth; the
origin check applies to cookie writes). Validation (422): `to_date ≥ from_date`, `to_date ≤` local
today, at most 366 days, no unknown field. Without `confirm` the response is
`{"ok", "estimate": {from_date, to_date, eligible_count, selected_count, truncated, max_rows,
memory_count, rule_count, ai_rows, ai_calls, batch_size, ai_available, ai_unavailable_reason}}`; it
computes the deterministic stage (D4 steps 1–2) on the selected rows without storing anything.
`ai_rows`/`ai_calls` are 0 when the model is unavailable. With `confirm: true` the estimate is
recomputed, the run inserted (`409 suggestion_run_active` on the partial unique index), and
`execute_run` scheduled as a FastAPI background task: `{"ok", "estimate", "run"}`.

`GET /api/review/suggestions/runs/latest` → `{"ok", "run": <run or null>}`. Both first mark a running
run whose lease expired as `failed`/`run_interrupted`.

*Rejected:* a separate estimate token that the confirm must echo. The estimate is cheap to recompute
and the confirm response returns the estimate it ran with.

### D4. Proposal order

1. **Memory** (`memory`): the owner's `merchant_memory` entry (source `owner`, active category) for
   `merchant:<row merchant>` or the recording event's stored `match_key` (merchant or signature key).
   For history a signature key may propose (unlike live ingestion, phase 3 D5), because the owner
   decides each suggestion. Category kind must match the row; an offset only for an offsetting income
   category and an eligible expense category. Reason "Sama dengan pilihan Anda untuk <merchant>" or
   "… untuk notifikasi bank serupa".
2. **Rules** (`rule`), on merchant + notes + the recording notification's title and text (read for
   keywords only, never stored, sent or logged):
   - repayment: income, current category not offsetting, a repayment word
     (`patungan|urunan|ganti(in)?( uang)?|bayar(in)?|balikin|split|tf makan|transfer makan|uang makan`,
     whole words, so "Pembayaran" does not match) → the owner's first active offsetting income
     category, notes "Ganti uang patungan", offset = the expense category the owner chose most often
     for repayments in feedback, else a meal category ("makan" in its name) when a meal word appears,
     else "Jajan & Nongkrong" when a café word appears, else none;
   - café: expense with a café, coffee or snack word and an active "Jajan & Nongkrong" that is not the
     current category → that category with its pillar.
3. **Model** (`ai`): the remaining rows in batches of `HISTORY_SUGGESTION_BATCH_SIZE` (D5).
4. **Skip**: a proposal equal to the row's current (merchant, notes, category, effective pillar,
   offset) — compared case- and whitespace-insensitively — counts as `unchanged`; equal to the row's
   pending suggestion counts as `unchanged`; equal to a rejected suggestion of the row counts as
   `rejected_before`. A different proposal supersedes the row's pending suggestion and is inserted.

*Rejected:* model first with memory in its context only. Memory hits are the owner's own answers and
cost nothing; asking the model again risks contradicting them.

### D5. Model batch

`OpenAINotificationProvider.suggest_history(context)` with prompt `prompts/history_suggestions.md`
(`HISTORY_PROMPT_VERSION = "history-suggestions-1"`) and the strict schema
`{"suggestions": [{"ref", "merchant", "description", "category_id", "kakeibo", "offset_category_id",
"confidence", "reason"}]}`. Context: `rows` (ref `r1…`, direction, amount, local date, account label,
merchant, notes, category, effective pillar, bank category from the stored text signature,
merchant_less), `categories` (active, of the batch's directions, with `default_kakeibo` and
`offsets_spending`), `offset_categories` (when income is present), `memory` (≤ 30 owner entries,
batch keys first) and `owner_history` (≤ 40 owner-classified rows, same merchants first). Every text
goes through `sanitize_text` with the owner's aliases; history is trimmed, then memory, to fit 60 KB.
Validation per item (D4 rules for references, pillar and offset); confidence < 0.5 dropped; merchant
null keeps the row's merchant; reason cut to 160 characters.

### D6. Run lifecycle and failure

`execute_run`: (1) deterministic stage under the lease: supersede stale suggestions, plan, store, count;
(2) if rows remain and the model is unavailable, `error_code` = `ai_disabled` or the configuration
code; (3) else batches: renew the lease, build the context, call the provider outside any database
transaction, validate, store; a `ProviderError` stops further calls and sets `error_code` to its code;
(4) `completed` with the code. Any other exception → `failed`/`run_failed`. Every write checks the
lease token, so a run that lost its lease stops writing. The lease (300 s) is renewed per step; a
restart leaves an expired lease, which the next read marks `run_interrupted`.
`processed_count` counts rows examined (deterministic proposals plus model batches); rows skipped
because the model was unavailable are not counted as processed.

Logs: one line per run with `error_code`, the number of invalid model items and unprocessed rows; no
names, notes, amounts or model output.

### D7. Supersession

`supersede_stale` runs before listing, accepting, rejecting and planning. A pending suggestion is
superseded when its row has `owner_edited_at`, `classified_by = owner`, a movement or fee link, or any
of type, merchant, trimmed notes, category, effective pillar or offset differs from the stored
`before_*`; or when the proposed category is archived or of the other kind. A review-only change
(phase 6 accept) changes none of these and keeps the suggestion.

*Rejected:* a trigger on `transactions`. It would run on every ledger write for a rarely used feature,
and the lazy check also catches category archiving.

### D8. Accept and reject

`POST /api/review/suggestions/accept {suggestion_ids: [1–200 distinct uuids]}`: one transaction under
`pg_advisory_xact_lock(notification:<user>)` (the lock of notification processing and bulk review);
supersede stale; lock the suggestions `FOR UPDATE` (any foreign or unknown id → 404
`suggestions_not_found`, nothing changes); lock the rows; validate the offset category; for rows whose
values change (`classification_change`), `snapshot_rows`, update merchant, notes, category, pillar
(NULL when equal to the category's pillar so the row keeps following it), offset,
`review_status = reviewed` and `OWNER_EDIT_ASSIGNMENTS`; unchanged rows only become reviewed; mark the
suggestions accepted; `record_owner_corrections(kind="accepted_suggestion", history_suggestions=…)`
writes feedback with the suggestion id, upserts owner memory for the event key and the merchant key,
and closes an open purpose question. Response `{"ok", "accepted", "skipped", "corrections"}`.

`POST /api/review/suggestions/reject` (same body): supersede stale, lock, 404 for foreign ids, mark
pending ones rejected. Response `{"ok", "rejected", "skipped"}`.

`GET /api/review/suggestions` → `{"ok", "truncated", "pending", "groups"}`; group key
`<type>|<merchant key or notes:<normalised notes>>|<category id>|<offset id>`, so a group's before → after
preview is true for each of its suggestions; groups sorted by size then total.

### D9. Settings

`HISTORY_SUGGESTION_MAX_ROWS` (1–2000, default 300) and `HISTORY_SUGGESTION_BATCH_SIZE` (1–50,
default 20). An invalid value sets `history_suggestion_configuration_error =
suggestion_configuration_invalid`; run requests then return 503 and the rest of the app is unaffected.
Defaults are mirrored in `.env.example`, Compose, `scripts/materialize_runtime_env.py` and the deploy
workflow.

### D10. Web section

`HistorySuggestionsSection` sits on `/review` below the queue and is shown also when the queue is empty.
Period inputs (native date fields, `noValidate` so the Indonesian messages from `windowError` are shown),
"Hitung perkiraan" → estimate box (`role="group"` named by its title) with "Mulai buat saran" and
"Batal" (focus returns to "Hitung perkiraan"); a zero estimate shows a single "Tutup". While a run is
running the latest run is polled every 2 s and shown as a labelled `<progress>` from real counts; when
it finishes the list reloads. Groups (`SuggestionGroupCard`) use a `<dl>` "Sebelum"/"Sesudah" preview,
text source chips (not colour), "Terima semua (n)"/"Tolak semua", and a `<details>` row list with
"Terima"/"Tolak" whose accessible names include the row's old label and date. Decisions are sent in
batches of 200; a partial failure states how many were done. Live tokens and the phase 6 card,
button and field styles are reused; no new visual language.

## Risks / Trade-offs

- [A suggestion accepted carelessly becomes wrong data and teaches memory] → Every acceptance is an
  explicit owner action with a before → after preview; feedback keeps the example; a later owner edit
  overwrites memory.
- [Keyword rules misfire, for example "ganti oli" on income] → They only propose; the owner rejects
  and the rejection is remembered for that row.
- [Model cost] → Estimate before start, a row limit and one call per batch; memory and rules first.
- [Long runs inside the API process] → Background task with a lease; a restart marks the run
  interrupted and the owner can start again (stored suggestions remain).
- [Rollback] → The previous release ignores both tables and the extra feedback kind; feedback rows of
  kind `accepted_suggestion` remain valid data. To remove V31: drop both tables and the column, then
  restore the CHECK constraint to two kinds after deleting or re-labelling the `accepted_suggestion`
  rows (an owner decision, because they are labelled examples).

## Migration Plan

1. Merge; the GitHub workflow deploys; Flyway applies V31; the mirror is a no-op.
2. Post-deploy (owner authority): `/review` shows "Saran perbaikan riwayat"; an estimate for the last
   month returns counts; a confirmed run completes and its suggestions can be accepted or rejected.
3. Rollback: redeploy the previous release (see Risks).

## Open decisions for owner

1. **Eligibility is narrower than `classified_by <> 'owner'`**: system rows (recurring, payroll,
   reconcile, investment), goal- and debt-linked rows and "Internal Movement"/"Investasi" rows are never
   proposed (D2).
2. **Signature memory may propose for history** (not for live ingestion): each suggestion is decided by
   the owner, so "at Groceries" → "Belanja dapur" is offered for old merchant-less payments.
3. **Rejections write no feedback row**; the rejected suggestion row is the record and is used to avoid
   re-proposing. Phase 9 can read it if rejections should become negative examples.
4. **Rules run before the model** and save its calls; with the model enabled, a rule proposal is not
   second-guessed by the model.
5. **Model proposals below 0.5 confidence are dropped**, not shown with a warning.
6. **A model merchant of null keeps the row's merchant**; suggestions never clear a merchant.
7. **Runs are processed inside the API process** as a background task (no separate worker); an
   interrupted run is not resumed automatically.
8. **The repayment offset is a best guess** (most frequent owner choice, else meal or café words); the
   owner sees it in the preview and can reject.
9. **Accepting an unchanged suggestion is impossible by construction** (equal proposals are not stored);
   if a row already equals the proposal when accepted, it only becomes reviewed and no feedback is written.
