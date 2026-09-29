# Design

## Context

See `proposal.md` for motivation and scope. Source inspection at `8315255` establishes these integration points:

- `backend/app/routers/ingest.py` receives the captured message fields, but its regex gate drops unfamiliar events before persistence. It performs parsing, account selection, broker effects, and ledger application in one batch database transaction. It returns only counters after commit.
- `notification_events` already has a unique `(user_id, payload_hash)`, raw message fields, `parsed_summary`, and one nullable `transaction_id`; it has no queue state, retry ownership, or durable mobile result.
- Ingestion currently loads archived categories, omits `default_pocket_id` from account context, falls back to the first account, creates unmatched named Jago pockets, and uses fixed owner/merchant/pocket hints. These cannot remain authoritative in the constrained processing path.
- `ledger_mutations.py` owns active-account locks, bilateral creation, settlement balance guards, and movement Kakeibo policy. The manual merge router contains reusable pair validation/linkage but commits within the route.
- Current monetary ledger columns are whole-IDR `BIGINT`; the regex amount helper truncates decimal fractions. Preserving fixed facts does not justify widening the application's monetary schema in this change.
- Flyway migrations in `db/migrations/` are the Compose/release path, currently through V19. `init_db.py` also maintains startup compatibility. Redis has persistence disabled and cannot be the sole durable event queue.
- The API lifespan already starts background jobs. The existing Zen adapter uses direct HTTPX calls; the Telegram service uses a separate agent loop, which is not suitable here.
- Older main ingestion/filtering specs describe a single `transfer` row and unrelated-account fallback. These deltas replace those contracts while preserving the approved financial-integrity change's canonical pairs, idempotency, reconciliation exception, and manual merge behavior. No older change is silently marked complete or archived.

## Goals / Non-Goals

**Goals:** Keep probabilistic interpretation outside the trusted write boundary; make accepted work recoverable; keep a small backend integration with independently measurable model quality.

**Non-goals:** No coding-agent runtime, tools, LLM-managed SQL, implicit learning-rule writes, new financial UI, mobile edits, general decimal-currency migration, historical ledger repair, production activation, or speculative multi-provider framework. Broker instrument provisioning remains deterministic and separate.

## Decisions

### 1. One processor, one OpenAI adapter, one validated domain contract

Introduce small services by responsibility: context preparation, notification interpretation, validated application, and processing-state management. Keep these as ordinary backend modules rather than a plugin framework. A provider interface accepts a sanitized context and returns structured interpretation; an OpenAI adapter uses the lifecycle-managed official `AsyncOpenAI` SDK and the Responses API. Pin `openai==3.19.2`, the latest release verified during planning, and do not use a generic OpenAI-compatible HTTP client.

Keep the prompt in a versioned repository resource alongside its output contract. A strict Pydantic model forbids unknown fields and constrains description length, classification enums, UUID references, and evidence fields. Output includes:

- Outcome: settled financial record, noise, or needs review; observed direction is separate from a possible movement hint.
- Concise Indonesian description, existing effective account/category IDs, optional source/target IDs, and transaction-level Kakeibo.
- Evidence identifying exact source text for the settled amount/currency and transfer endpoints; an optional candidate transaction ID refers only to the supplied candidate set.
- Confidence and review reason are diagnostic hints, never write authorization.

Amount evidence is verified independently; the model does not supply an authoritative amount or date. Use Responses API Structured Outputs with a strict JSON Schema derived from the Pydantic contract, `store=false`, no tools, and `gpt-5.6-luna`. SDK responses that are incomplete, refused, malformed, or schema-invalid result in a safe review outcome rather than permissive extraction. Do not repair JSON by inventing values or repeatedly ask the model to reinterpret until a convenient answer appears.

Alternative rejected: reusing the Telegram agent loop, a generic provider framework, or exposing merge tools to the model. Each expands authority without improving this narrow interpretation task.

### 2. Explicit model/privacy policy and opt-in activation

Operational configuration permits only `gpt-5.6-luna`. Its default reasoning effort is `low`; `none` is allowed solely as an explicit latency baseline in configuration and evaluation. No automatic model or reasoning fallback. This selected model supports the Responses API and structured outputs. It is cost-sensitive but not free; the owner supplies the OpenAI API credential and approves the resulting API usage.

Configuration names include `NOTIFICATION_AI_ENABLED` (false by default), `NOTIFICATION_AI_MODEL`, `NOTIFICATION_AI_REASONING_EFFORT`, `OPENAI_API_KEY`, and bounded history/timeout controls. Enabling AI requires the configured operational model, permitted reasoning level, and credential through normal runtime configuration. Invalid model/effort or a missing credential fails the processor configuration, not unrelated API startup. Disabled inference uses the safe deterministic path where supported; unresolved events remain reviewable rather than falling back to arbitrary references. Temporarily unavailable enabled inference queues/retries work instead of silently reverting to heuristic financial writes.

OpenAI API requests are not used for model training by default. With `store=false`, the Responses API does not retain application state for this request, but the standard service retains abuse-monitoring logs for up to 30 days. Zero Data Retention or Modified Abuse Monitoring require separate OpenAI approval and are not assumed. Before activation, recheck the configured project's current OpenAI model access, spend limits, and retention controls with synthetic requests. If the selected model does not accept ordinary API requests, leave AI disabled, record the blocker, and seek an approved provider decision; do not select a different model.

Public research supporting this decision: [GPT-5.6 Luna](https://developers.openai.com/api/docs/models/gpt-5.6-luna) supports Responses, Structured Outputs, and `none`/`low` reasoning. [OpenAI data controls](https://developers.openai.com/api/docs/guides/your-data) documents no-training-by-default, `store=false`, and standard abuse-monitoring retention. Neither establishes measured Indonesian parsing quality; that remains subject to the synthetic evaluation gates below.

### 3. Minimized dynamic context and evidence preservation

Use supported packages as institution evidence, not a fixed owner's account/pocket/category dictionary. Include active owned accounts with hierarchy, explicit default pocket/funding references, type/instrument eligibility, and savings designation. Only active categories of compatible kinds are selectable; category names do not fix Kakeibo. Applicable owned merchant rules are hints, not inferred new rules.

Expose the uniquely resolved observed account/default pocket, or Jago movement endpoints, as backend-owned fact IDs in the model context using the same resolver as application. Expose an explicit mapping error instead of guessing when resolution is ambiguous. The model must copy resolved IDs and exact contiguous evidence quotes, including redaction placeholders; trusted validation still independently rejects incompatible proposals.

Recent history defaults to 20 relevant records, bounded to 0–50. Read from the database, not frontend pagination. Select movement candidates in a separate indexed query around the event's original timestamp. Candidate queries include notification evidence and both legs of eligible existing movements; never truncate a set in a way that converts ambiguity into a unique match.

Bound raw text, total context, history, candidate count, and output. Prefer relevant institution hierarchy and candidate endpoints when context is large, explicitly indicate omitted context, and require review when complete ambiguity checks or essential mapping cannot fit the limit. Do not send a whole database or assume every user has a small account list.

Send a normalized message assembled from relevant title/body/big-text/subtext/summary, not arbitrary `raw_extras`. Exclude API/session secrets, notification/device identifiers, account numbers, OTPs, receipts, full raw transaction notes, and unrelated profile metadata. Backend-owned self-identity comparisons use the current user's profile and replace personal identity in outbound text with an evidence marker; no literal owner's name is embedded in code/prompts. Registered account/category display names and sanitized merchant descriptions remain where needed for the requested interpretation. Preserve originals inside the already authorized owner-scoped ingestion storage; do not output them in tooling or diagnostics.

Use `Decimal` to normalize currency-marked amount evidence. Distinguish principal, fees, balance, and total; only an independently supported settlement amount can be applied. Integral decimal notation such as `500,000.00` is supported; actual fractions produce review because the current ledger cannot preserve them. Mobile `expected_amount` is only a hint and cannot resolve missing/conflicting evidence alone. Supported deterministic broker quantity/price calculations remain separate.

Preserve `post_time` as the canonical event instant, with timezone and seconds; never substitute inference time or round to the minute. A model cannot change it. Unknown format, conflicting amounts/direction, unsupported currency, and insufficient settlement evidence produce explicit review/noise rather than guessed transactions.

### 4. Durable processing in the existing event table

Use an additive migration after V19, reserving V20 unless another approved migration takes it before implementation. Add nullable/default-compatible fields to `notification_events` for processing state, mode, attempt count, next attempt, lease token/expiry, processing generation, safe error code, provider/model/prompt version, movement ID, interpretation/evidence kind, observed direction, stable result key, and compact committed result snapshot. Add queue/candidate indexes and a uniqueness guard for direct observed confirmation of each movement role. Keep the existing `(user_id, payload_hash)` claim and `transaction_id` anchor.

An explicit complete movement event anchors the logical operation but is distinguished from a directly observed single-direction leg. A later direct role confirmation can claim that role once. Plain observed expense/income links remain distinct from synthesized/complete movement evidence. Do not replace this provenance with a broad fuzzy duplicate matcher.

State transitions:

```text
accepted --> queued --> processing --> recorded
                            |       --> ignored
                            |       --> needs_review
                            |       --> queued (transient retry)
                            +------ --> failed (permanent/exhausted)

needs_review / failed --> queued (explicit owner retry)
expired processing   --> queued (lease recovery)
```

Known empty/unsupported/noise events are ignored before persistence. Persist supported unfamiliar candidates before external inference; regex `noise` alone is not proof of non-financial text. Claim accepted events per short transaction. A PostgreSQL failure means acceptance failed, not a successful queued response. Each event has its own application transaction so one invalid event cannot undo unrelated accepted work. Ordinary duplicate submission reads existing state, preserving original payload and not resetting retry budgets.

Start a bounded worker loop in API lifespan only when processor settings permit. A short `FOR UPDATE SKIP LOCKED` claim transaction sets a random fencing token, increments generation/attempt count, and commits. Close the database connection before inference. Read context in a separate short transaction, perform inference with a deadline, then enter a new application transaction. The worker revalidates the current token/generation/lease before any effect. A stale worker cannot apply its result after another worker recovers the lease.

Explicit retry keeps an event's original deterministic or AI processing mode. A deterministic event can be retried while optional AI is disabled; an AI event returns a conflict without changing state when its worker configuration is unavailable. Legacy events without a recorded mode choose the supported broker deterministic route or AI only when AI is available. This prevents a successful-looking queued result that no running worker can claim.

Initial conservative limits: one in-flight inference per API process, 30-second total call deadline, 90-second lease, three transient attempts, exponential backoff with jitter and honored `Retry-After`, and bounded output. These are initial defaults to measure in synthetic runs, not performance promises. Retry transport timeouts, 429, and transient 5xx; authentication/access/model-policy failures are terminal `failed`; semantic uncertainty or invalid structured output is `needs_review`. Provider exception bodies are never logged. Shutdown cancels the loop/client before closing the database pool; accepted leases remain recoverable. No in-memory task or Redis entry is the sole source of truth.

### 5. Trusted ledger application and shared movement linkage

Refactor the manual merge route's pair validation/mutation into a cursor-based service with no internal commit. The manual route retains the existing public behavior, including no 30-second restriction. Notification application adds its stricter evidence/time rules around the shared primitive.

Within the event application transaction, re-read active owned references, category kind, effective default pocket, and financial association eligibility. Take a short per-user advisory transaction lock for notification movement discovery/application, then acquire ledger/account row locks in stable order. Requery candidate uniqueness under that lock, so two concurrently inferred events cannot both miss a counterpart or consume it twice. A model-selected candidate is not privileged over contradictory current evidence.

Three application cases:

1. **Ordinary observed expense/income:** create only its observed direction using source-backed amount and original timestamp. Store the AI-generated description in the established transaction notes field; store provenance/interpretation separately. A strongly evidenced but incomplete self-transfer can use the existing canonical internal-movement category with no fake counterpart and a pending movement hint. No arbitrary Kakeibo saving attribution.
2. **Complete Jago pocket movement:** resolve two distinct active liquid endpoints under the same registered parent. Single-pocket syntax resolves an implicit main endpoint only from valid configured/unique main context. Use `create_bilateral_movement(..., allow_negative=True)` atomically and record complete-movement provenance and one result key. Never auto-create a generic liquid pocket. Ordinary Jago expenses/income continue to use their named effective pocket where supported.
3. **Cross-account corroboration:** find a mutually unique compatible opposite observed leg with equal positive amount/currency, different owned eligible liquid accounts, and `abs(event_time - candidate_event_time) < 30 seconds`. Require transfer/endpoint evidence, not just equal values or an AI guess. Create the current observed leg if absent and link it to the existing counterpart using the shared merge primitive, with canonical movement classification and no further pair creation. Preserve the counterpart's financial values/notes/receipts. For a uniquely evidenced existing complete movement with an unconsumed role, attach the confirming event without a new transaction. Exclude manually recorded/financially associated rows from automatic conversion.

Evidence compatibility includes explicit registered endpoint/package relationships, supported bank reference tokens where present, or corroborating self-transfer identity determined from current user data. A masked sender name alone is not proof. Multiple candidates, fees that make amounts unequal, exactly 30 seconds, unsupported/foreign references, or insufficient provenance cannot auto-link. Timing uses original event instants, not arrival or model execution time. Any bounded-query overflow counts as ambiguity.

If interpretation of the current observed leg is sound but movement linkage is ambiguous, record that leg once and leave it unlinked with safe review metadata; if the leg itself is uncertain, produce `needs_review` without a write. Do not mark an independently committed leg as failed just because a possible movement could not be proven.

Canonical Kakeibo comes from `movement_kakeibo`: operational transfers are neutral, crossing a savings boundary uses the existing signed economic policy, trades stay excluded. Settled expenses and movements can exceed a stale tracked balance only via ingestion's existing `allow_negative` exception, setting reconciliation metadata atomically. Manual operations retain overdraft protection.

The deterministic Stockbit branch continues to handle supported quantity/price/symbol facts, configured broker funding, instrument provisioning, and trade effects. Move it behind the same event claim/result transaction boundary without letting the AI choose new instruments or quantities. Missing/ambiguous broker/funding configuration becomes review rather than a generic liquid fallback. Preserve the existing trade contract and add regressions; broader pre-existing investment defects are not claimed fixed by this change.

### 6. Compact committed results and companion compatibility

Keep request fields and batch counter keys. Return one `results` item per input event, including definite ignored events. `inserted` counts events newly accepted by that request, `updated` counts recognized redeliveries, and `created_transactions` counts only ledger rows synchronously committed during that request. Asynchronous acceptance normally returns zero for created transactions; do not manufacture future totals.

Recorded item example (existing counters omitted here only for brevity):

```json
{
  "ok": true,
  "results": [
    {
      "payload_hash": "synthetic-payload-hash",
      "event_id": "00000000-0000-4000-8000-000000000001",
      "status": "recorded",
      "record_key": "notification:00000000-0000-4000-8000-000000000001",
      "type": "internal_movement",
      "description": "Pindah saldo ke Dana Darurat",
      "source": "Bank Jago · Kantong Utama",
      "target": "Bank Jago · Dana Darurat",
      "amount": 500000,
      "currency": "IDR"
    }
  ]
}
```

Queued/review/failure results need only `payload_hash`, `event_id` where persisted, `status`, and a safe `error_code` when actionable. Do not send raw payloads, confidence traces, account objects, or full transaction lists. `record_key` and monetary success fields are absent until commitment. Plain expenses have `target = null`; an income's owned receiving account is `target`, with an external sender label in `source` only if supported and sanitized. Unknown external labels are null, never invented owned accounts. The special trade result identifies its deterministic source/target under the existing movement semantics, not an AI-created trade.

Persist the compact snapshot and ledger effect in the same transaction. Generate the stable key from the first committed event identity; when a later event links into that logical movement, inherit that key and update both snapshots' movement presentation atomically. Retries and counterpart confirmations reuse it. The snapshot confirms a historical commit, not continued existence after a subsequent deliberate user deletion; deleting a ledger record must never requeue the event or cause replay.

Add owner-authenticated `GET /api/ingest/notifications/{event_id}/result` returning `{ok: true, result: ...}` and `POST /api/ingest/notifications/{event_id}/retry` for `needs_review`/`failed` only. Retry bumps generation, clears safe error state, and queues the same captured event with fresh registered context; it does not reinterpret mutable label hints as authoritative financial evidence. Recorded/ignored retries return conflict, foreign events return not found. Existing label/list operations gain safe status/error metadata but no implicit ledger writes. Cookie requests retain existing CSRF checks; Bearer API keys use the same ownership dependency.

This change supplies a backend completion contract, not mobile polling or notification-channel implementation. Existing clients that read only counters remain compatible but cannot infer recorded success from a queued acknowledgement. Document this prominently for the mobile owner.

### 7. Synthetic evaluation with explicit scoring and safety gates

Create a repository-owned synthetic fixture set with at least 40 distinct cases and expected outcomes, covering supported package variants, Indonesian/English phrasing, renamed institutions/custom pockets/categories, unknown and archived references, missing history, promotions/OTP/pending/failed messages, amounts with separators/fees/fractions, seconds/timezones, Jago single/dual-pocket movement, cross-account order/retries, 29-second and 30-second boundaries, ambiguity, and prompt injection. Use fictional people, account names, references, and IDs generated for fixtures; do not copy existing real-person examples or export runtime data.

Build the runner so it imports only synthetic fixture context and the processor/adapter, not runtime database loaders. It SHALL first execute the same fact collection/routing boundary as production: definite ignored/non-candidate events are scored as deterministic entrance-filter cases with no provider call; supported Stockbit cases are marked as deterministic broker routing with no provider call and remain covered by their application tests; only candidate non-Stockbit events are sent to the model and included in model-quality metrics/gates. A failed deterministic route is reported separately and prevents a passing evaluation, but it cannot distort AI quality metrics.

Live runs are explicitly requested, non-mutating evaluations using the normal OpenAI credential mechanism with no credential output. They never write the real ledger. Run `gpt-5.6-luna` with `low` reasoning and, when explicitly requested, `none` using three repeats per model-eligible case/configuration, bounded request rate, and the same prompt/schema. Report model, reasoning effort, usage when returned, and unsupported parameter differences. The runner SHALL estimate each request conservatively from UTF-8 input bytes plus the configured maximum output tokens using the current published Luna text prices, and refuse a run exceeding its explicit per-run cap. Default the cap to $0.25, which is deliberately below the owner's $5 total evaluation budget; report the estimated ceiling and observed token-priced usage, while noting that OpenAI billing remains the account-level source of truth. Mocked CI exercises the runner/scorer with no external call.

For every run record code revision/dirty-state identity, fixture and prompt versions/hashes, exact model ID, run time, request settings, offered/completed/failing counts, repetitions, and latency p50/p95. Report field-level precision/recall and a 0–100 composite score: schema validity 10%, source-backed facts 25%, reference mapping 20%, category/Kakeibo 20%, movement decisions 15%, and safe abstention 10%. Score absent/failed answers as failures. Describe description quality separately with a small deterministic rubric plus owner sampling; do not substitute a model judge for financial ground truth.

For synthetic model-eligible rows only, report the raw model proposal, trusted validated decision, and safe validation rejection code separately. A rejected unsafe proposal is a model error, not a correct answer; successful rejection demonstrates the application guard, not model accuracy. Apply the operational write-safety gate to validated decisions, and include the raw error/rejection rate when judging usefulness. The report may contain these fixture values because they are repository-owned fictional data; it SHALL never emit equivalent runtime data.

Operational evaluation gates: no wrong committed fact/reference, no false movement merge, no writes for explicit unsafe/noise cases, schema validity at least 98%, and category/Kakeibo accuracy at least 90% on unambiguous fixture cases. Report abstentions and movement recall so a model that always refuses cannot pass as useful. A failed safety gate overrides the composite score. These are acceptance targets, not claims about any current model; passing synthetic cases is not proof of zero real-world errors.

Tuning uses a defined development subset; retain a held-out subset for final comparison. If prompt/schema/fixtures change after a benchmark, rerun affected comparisons with the same revision rather than comparing different prompts. A `none` baseline never automatically replaces the `low` operational default.

### 8. Owner-authorized local dry runs are separate from benchmarking and ingestion

The synthetic runner continues to refuse runtime records. Add two owner-authenticated dry-run routes instead: `POST /api/ingest/notifications/dry-run` accepts one ordinary mobile event payload without accepting/persisting it, while `POST /api/ingest/notifications/{event_id}/dry-run` reads exactly one existing owner-scoped local event. Both use current owned context, the same fact collection, model allowlist, outbound redaction, and trusted validation as ordinary AI processing, but they never create an event, claim a lease, enqueue/retry work, apply a transaction, or mutate any balance/state. They do not enable or start the ordinary worker.

Permit these routes only when both `APP_ENV=development` and a new disabled-by-default `NOTIFICATION_AI_DRY_RUN_ENABLED` flag are configured. Every other environment, including production, returns a safe not-found response before loading event/context or calling the provider. Require existing session/Bearer owner authentication; stored-event lookup is owner-scoped and cross-user identifiers return not found. Direct payloads use the ordinary event schema but bypass idempotency/persistence entirely.

Use read-only database transactions for direct/stored context reads and original captured facts rather than mutable inspection labels. In development dry-run-only mode with operational AI disabled, suspend notification, recurring, and market-price background jobs so unrelated automatic financial updates cannot contaminate no-write evidence. Preserve non-record model outcomes before constructing a proposal; share endpoint/category resolution with ordinary application, without claiming that a proposal proves counterpart merging or application-time locking.

The compact dry-run response identifies only a `dry_run` outcome (`would_record`, `ignored`, `needs_review`, `failed`, or `not_ai_eligible`), proposed description/type/source/target/amount/currency/category/Kakeibo when available, and a safe validation/provider error code. It contains no raw notification text, IDs from unrelated records, provider body, credential, or stored diagnostic trace. The authenticated local caller can compare this response with the notification on-device, while derived general patterns—not raw text—become future fictional benchmark cases. Stockbit and entrance-filter events use their deterministic routing result and never call OpenAI.

Alternative rejected: enabling `NOTIFICATION_AI_ENABLED` in local Compose and resubmitting stored events. That could cause the worker to claim unrelated queued notifications and create ledger effects, so it does not meet no-write test isolation.

## Risks / Trade-offs

- [OpenAI credential, project spend limit, or model access can reject ordinary API use] → Check first with synthetic traffic; remain disabled/recoverable on denial and never switch models silently.
- [Standard OpenAI API monitoring can retain financial-context content for up to 30 days] → Keep outbound minimization/redaction, use `store=false`, document the boundary, and require separate OpenAI approval before claiming stronger retention controls.
- [Pricing/model documents can change] → Recheck before activation and model changes; operational allowlist and explicit activation are not a promise of permanent vendor terms.
- [Probabilistic classification or JSON errors] → Independent evidence/reference validation, safe review, held-out synthetic evaluation, no agent tools.
- [Whole-IDR schema cannot preserve fractional amounts] → Review those events without rounding; general decimal-money support needs another change.
- [Async acceptance differs from the existing synchronous mobile assumption] → Keep counters and distinguish `queued` from `recorded`; expose completion lookup and document the boundary without changing mobile code here.
- [Multiple plausible equal-value movements] → Full mutual uniqueness and compatible evidence under serialized short application, strict time window, and no ambiguity-changing truncation.
- [Provider latency/rate limits] → Bounded durable worker, deadlines/backoff, no held DB locks, measured latency and explicit failures.
- [A local dry-run route is accidentally exposed or writes data] → Require development environment plus a separate flag, existing owner authentication, an early production-safe rejection, and mutation-spy/database assertions that direct/stored tests leave all rows and balances unchanged.
- [Legacy broker logic and historical events lack new provenance] → Preserve separate deterministic handling and avoid automatic historical replay; report any discovered pre-existing defect separately.
- [No new review UI] → Existing owner event inspection, explicit backend retry, and ordinary ledger/manual merge stay available; a dedicated review screen is deferred.

## Migration Plan

1. Implement and verify an additive migration after the current highest version; never edit checksummed V12/V13 or earlier migrations. Keep supported startup initialization in parity without turning startup into a historical ledger rewrite.
2. Existing events with a linked transaction are recognized as already recorded without external inference. Materialize their compact results lazily from existing data; legacy movement keys derive from existing movement identity. Existing unlinked events become reviewable, not automatically queued. Do not backfill guessed provenance or repair old duplicate ledger pairs.
3. Test migration on fresh isolated PostgreSQL and a V19 baseline with synthetic existing notifications, transactions, linked movements, and archived references. Verify old clients/disabled AI, repeated migration validation, and unchanged monetary state.
4. Ship AI disabled by default. After ordinary direct access and synthetic evaluation pass, activate only in the authorized local environment through runtime configuration. Production release/credentials/activation remain a separate owner-authorized pipeline action.
5. On rollback, stop workers and disable AI before reverting application code. Leave additive state columns/indexes and accepted data intact; do not drop or replay records. Preserve recorded results/idempotency and document that the older application lacks queue recovery. A later redeploy can resume only uncommitted accepted work with fresh leases.

## Open Questions

- What timeout/rate/history limits best fit measured synthetic latency and the owner's eventual volume? Conservative defaults are defined above and can be tuned without relaxing validation.
