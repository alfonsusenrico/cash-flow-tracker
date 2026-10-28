# Automation quality audit: capture → ledger → monthly report

Date: 2026-10-28. Scope: cash-flow-tracker (`main` at 08b0bbd, owner checkout on
`fix/investment-topup-label`) and financial-tracker-mobile-listener (`feat/swipe-to-delete`,
19 commits ahead of `main`, no git remote at the time). Read-only code audit. Key findings were
re-verified on `origin/main` fab3811 the same day. A read-only production and device evidence pass
followed. It confirmed B3 against the live constraint, and showed that C6 is latent: production
rows currently agree on their pillar. Its figures stay out of this tracked note.

Goal stated by the owner: the phone captures and the backend + LLM records every transaction
with the correct type, category, need/want pillar, and an informative name, so a weekly or
monthly review in the web UI is quick and the reports are trustworthy.

## 1. Findings by symptom

### A. Non-transactional notifications are kept

| # | Finding | Evidence |
|---|---|---|
| A1 | The phone forwards every notification from enabled packages. Only one-time codes are dropped. It has no negative classifier; the regex hint extractor only adds hints. | `NotificationProcessor.kt:54-91` |
| A2 | `com.gojek.app` (Gojek superapp: rides, food, chat, promos), `com.gopay.wallet` and `id.co.bca.*` are captured but cannot be toggled in Settings. | `FinancialSourceRegistry.kt:17-130`, `AppPreferences.kt:110-118` |
| A3 | Group summaries, ongoing/progress notifications and re-posts of the same `sbn.key` are each stored as new rows. `payload_hash` includes `postTime`, so every update gets a new hash. | `NotificationProcessor.kt:124-134` |
| A4 | On the phone, `needs_review` and `failed` rows are kept forever. Ignored rows are kept for 3 days. | `NotificationDao.kt:252-266` |
| A5 | On the server, raw text plus `raw_extras` JSONB is kept forever for every stored event, with no purge. Model-ignored events are stored without their reason. | `notification_application.py:197-200` |

### B. Transactions not recorded, or recorded half

| # | Finding | Evidence |
|---|---|---|
| B1 | **Silent loss.** The phone OTP regex matches a bare `PIN`. A payment notification ending "jangan berikan PIN…" is dropped before storage, leaving no trace. | `NotificationProcessor.kt:116-118` |
| B2 | **Silent loss.** The server `NOISE_PATTERN` treats a match on `cashback`, `promo`, `diskon`, `pending` and similar words as noise. A settled payment mentioning cashback returns `ignored` before any insert, so nothing is stored. | `notification_evidence.py:33-38,162`; `notification_processing.py:145` |
| B3 | **Likely insert failure (traced in code, not run).** The AI schema and `/resolve` accept `culture` and `unexpected`. The only DB check on `transactions.kakeibo_type` allows `need`, `want` and `saving`. An insert with those values would violate the check, be retried as transient, and end as `failed`. Production's live constraint is still to be confirmed. | `notification_interpretation.py:14`, `ingest.py:54`, `V11:19-20` |
| B4 | **Fees break pairing.** Movement legs pair only on exact amount equality within ±15 min. A BI-Fast or admin fee, or a slow second leg, leaves the move unpaired, and nothing tries to pair it again later. | `notification_context.py:133-165` |
| B5 | **Half-recorded movements.** A one-sided self-transfer is recorded as an excluded "Internal Movement" expense, and the destination account is never credited. | `notification_application.py:232-339` |
| B6 | A second Rp amount (fee, limit) produces `ambiguous_amount` and goes to review. Refund or reversal wording matches neither the settled nor the direction patterns. | `notification_evidence.py` |
| B7 | A model `ignored` answer is final, so a settled candidate can be dropped without review. | `notification_processing.py:306-307` |
| B8 | Phone reliability. The insert is fire-and-forget. One 4xx response poisons a whole batch of 50. Notifications posted while HiOS has the app frozen and then dismissed are lost. | `NotificationProcessor.kt:94-109`, `NotificationSubmission.kt:88-93` |

### C. Generic names, wrong type, category or pillar

| # | Finding | Evidence |
|---|---|---|
| C1 | There is no merchant or description field. Everything goes into `transactions.notes`. | `V1:49` |
| C2 | Parser placeholders become names: "BCA Payment", "QRIS Merchant", "Debit", "Transaksi bank", "Bank Jago Payment". The prompt also permits "a concise generic purpose". | `notification_parser.py:567,588`; `prompts/notification_interpretation.md:9-11` |
| C3 | Every Jago card or QRIS payment gets the category hint "Tagihan & Utilitas". | `notification_parser.py:368,393` |
| C4 | Linking a movement overwrites the notes with "Pindah saldo ke X". The original merchant name is not restored when the movement is split. | `movement_linkage.py:65-77`, `movements.py:236-258` |
| C5 | **No learning loop.** Nothing writes `merchant_category_rules`. User edits never teach the system. The LLM sees category names without their pillar defaults, and sees the last 20 transactions of any kind rather than similar ones. The model's `confidence` is never used. | `notification_context.py:41-130` |
| C6 | **The pillar has three definitions.** The ledger uses `COALESCE(t, category, is_primary)`. The 50/30/20 report uses `COALESCE(t, 'need')`. Pulse uses the category pillar. Recurring, reconcile and deterministic-path rows have a NULL pillar, so the report counts them as **need** even when their category is a want. | `transactions.py:211`, `dashboard.py:150`, `pulse.py:101` |
| C7 | Uncategorised and archived-category spending drops out of the category breakdown but stays in total outflow, and there is no "Tanpa kategori" bucket. Reconcile adjustments are uncategorised and count as need. | `dashboard.py:598-608`, `accounts.py:779-795` |
| C8 | The insights badge compares against `"SEHAT"`, but the backend returns `healthy`, `warning` or `elevated_needs`. The badge therefore never shows healthy. | `insights/page.tsx:971`, `dashboard.py:226` |
| C9 | A ShopeePay top-up whose BCA leg is missing becomes income, which inflates income. | parser + pairing gap |

### D. Telling automated rows from manual ones

- `transactions` has no `source` column. Provenance can only be inferred:
  - from `notification_events.transaction_id`, which links an event back to its transaction;
  - from `idempotency_key` prefixes: `notification:`, `recurring:`, `payroll:`, `:out` and `:in`;
  - from `recurring_rule_id` and `movement_id`.
- Manual web rows and Telegram rows are indistinguishable.
- Nothing records "auto-created, then edited by the owner". `updated_at` is the only hint, and movement linking does not bump it.

### E. Review UX and dead code

- **No web review queue.** Review endpoints are mobile-only. There is no bulk recategorise, no "apply to similar", and no date or calendar-month range in the ledger.
- **Dead modules:**
  - `services/ledger/*`, `services/metrics.py`, `services/projection.py`, `services/categories.py` and `models/public.py` target an old schema.
  - `services/category_rules.py` is test-only and hardcodes the owner's name.
- **Telegram bot.** It is deployed but broken against the current API: transfer gives 422, and edit creates a duplicate.
- **Schema DDL runs in two places:** Flyway, plus `init_db_schema()` at startup.
- **Mobile repo.** It has no remote, and the phone runs an unmerged feature branch.

## 2. Diagnosis

The ledger core (movements, obligations, goals, investments, roughly 700 tests) is sound. The
quality problems come from four design choices in the automation layer:

1. **Filtering is backwards.** The phone sends everything, and the server's keyword rules both
   discard real payments (B1, B2) and keep noise (A1–A5).
2. **The pipeline is binary: record perfectly, or hold.** Anything uncertain leaves the ledger
   and becomes a mobile-only `needs_review`. A safer contract is that every settled amount gets
   a ledger row, and uncertain rows carry a visible review flag.
3. **The LLM fills a form but never learns.** It gets no merchant memory, no similar-history
   retrieval, and no pillar context, its confidence is unused, and corrections are discarded.
4. **There is no single definition of a reportable expense or its pillar.** As a result, the
   monthly report is wrong even when individual rows look right (C6, C7).

## 3. Target design

### Phone: capture layer refactor, not a rewrite

- Classify on the device into three outcomes:
  - **drop:** OTP, using a code-shaped regex rather than the word `PIN`; ongoing notifications; group summaries; known non-financial channels;
  - **forward:** an amount plus a settlement or direction verb;
  - **local quarantine:** never sent, with a counter and a "send anyway" action.
- Write durably and synchronously before `onNotificationPosted` returns.
- Key rows by `sbn.key` plus a content hash, so a re-post updates the existing row.
- Read `EXTRA_TEXT_LINES` and MessagingStyle text.
- Isolate a failing row instead of failing the whole batch.
- Expose every package's toggle in Settings.

### Server: an ingestion pipeline that records and flags

1. **Facts (rules).** Amount, direction, fee, balance, reference, account endpoint. Noise rules
   only skip what is clearly not settled, never a payment that mentions cashback.
2. **Enrichment (LLM)**, producing:
   - a normalised `merchant`;
   - a human `description` saying what was bought or paid, never a placeholder;
   - `category`, `pillar` and `confidence`.

   Its context includes:
   - categories with their default pillars;
   - learned merchant rules;
   - the top-k similar past transactions with the owner's final labels.
3. **Memory.** Every owner edit to category, pillar or name on a row with a merchant upserts a
   merchant rule. A rule hit skips the LLM entirely, so it costs nothing and gives a
   deterministic result.
4. **Gate.** A rule hit or high confidence posts as `auto`. Otherwise the row posts with
   `review_status = needs_review`. Nothing leaves the ledger, except notifications that are
   not settled.
5. **Pairing.** Allow a fee tolerance from a fee table. Run a deferred re-pairing pass over
   24–48 h, and include manual rows as candidates. A one-sided self-transfer becomes a pending
   movement that asks only for its counterpart.

### Data model (migration with backfill)

- New `transactions` columns:
  - `source`: `manual`, `notification`, `recurring`, `payroll`, `movement`, `reconcile`, `bot`, `import`;
  - `merchant`;
  - `description`, which keeps `notes` for the owner's own notes;
  - `review_status`: `auto`, `needs_review`, `reviewed`;
  - `classified_by`: `rule`, `ai`, `owner`;
  - `classification_confidence`;
  - `owner_edited_at`.
- Backfill `source` from the event links and key prefixes in section 1D.
- **One pillar definition:** `COALESCE(t.kakeibo_type, c.kakeibo_type)`, implemented as one
  SQL view or function that every report uses. Align the DB constraint with whichever pillar
  set the owner chooses.
- Purge raw event text after N days for ignored and recorded events. Keep the facts and the
  outcome.

### Web: a review inbox (minimal UI change)

- A "Tinjau" queue of auto rows since the last review, grouped by merchant. Supported actions:
  - accept all;
  - recategorise all, which also writes the rule;
  - rename.
- A monthly report:
  - uses the single pillar definition;
  - shows a "Tanpa kategori" bucket;
  - drills down to the ledger rows behind each figure;
  - offers calendar-month and cycle views.

### History fix

- A one-time AI re-enrichment of past transactions, written as *proposals*. The owner bulk-accepts or rejects them per merchant, so past months' reports are corrected too.

## 4. Recommended sequencing (each item an OpenSpec change)

0. **Production evidence pass (read-only).** Measure each finding before planning.
1. **Correctness fixes.** These are small and independent:
   - B1 (OTP regex) and B2 (cashback noise);
   - B3 (pillar constraint, after confirming it on production);
   - C6 (report pillar definition) and C8 (insights badge);
   - NULL pillars on recurring rows.
2. **Provenance and review data model**, with the backfill.
3. **Ingestion pipeline:** record-and-flag, merchant memory, LLM enrichment.
4. **Mobile capture layer.** Release it after its backend, per the existing release-order rule.
5. **Movement pairing:** fee tolerance, deferred re-pairing, pending counterpart.
6. **Web review inbox and report fixes.**
7. **History re-enrichment proposals.**
8. **Cleanup:**
   - remove dead modules;
   - decide on the Telegram bot;
   - add a remote for the mobile repo and merge its `main`;
   - archive finished OpenSpec changes.

## 5. Production queries to run (read-only, aggregates only)

- Transactions per month by inferred source (event-linked, key prefix, recurring, movement, NULL).
- Event-linked transactions with `updated_at > created_at + 1 min`: these approximate the rows
  the owner corrected after automation.
- `notification_events` by `processing_state`, `error_code` and `provenance_kind`, plus the age
  of the `needs_review` and `failed` backlog.
- The live check constraint on `transactions.kakeibo_type`, and `failed` events with
  `database_unavailable`.
- The top 30 `notes` values on event-linked rows, to measure how many names are generic.
- Share of rows that are uncategorised, archived-category, or have a NULL pillar in the last
  three cycles. Compare the need/want split under the report's definition and the ledger's.
- Movements with a single leg, and Internal Movement expenses that have no `movement_id`.
- Size of `notification_events` and of its raw-text columns.
