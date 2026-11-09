## Context

See proposal.md (Why). This branch starts from `origin/main` `a92b66a`. The benchmark that failed ran on `af8f95e`
(clean), `gpt-6-luna`, effort `none`, `max_output_tokens` 2048, `store: false`, fixtures `synthetic-notifications-5`
and `-5-en`, development partition, 3 repeats. Raw reports: `cash-flow-tracker-prototypes/research/evidence/english-benchmark/bench-{en,id}-dev.json`
(owner-local, synthetic). No paid or real AI call was made for this plan.

### The path that produces `provider_invalid_output`

`OpenAINotificationProvider.respond` sends a Responses API request with `text.format` = `json_schema`, `strict: true`
and the schema from `Interpretation.model_json_schema()` passed through `strict_json_schema` (every property required,
`additionalProperties: false`, defaults removed). The schema keeps pydantic's `maxLength` (description 160, merchant
120, amount evidence 100, direction evidence 400, source/target evidence 200), `format: uuid`, `minimum`/`maximum` on
confidence and `maxItems: 3` on `mappings` and `alternatives`. `provider_invalid_output` is raised in three places:

1. `respond`: `response.status != "completed"` (incomplete: `max_output_tokens`, `content_filter`, ...) or empty
   `output_text`. The SDK's `output_text` joins only `output_text` parts, so a refusal item yields `""`.
2. `interpret`: `Interpretation.model_validate_json(output)` raises. Pydantic runs the field constraints and the
   `check_record_shape` model validator, which, for `outcome = record` only, requires a non-blank description, a
   direction and amount evidence; for a movement two distinct endpoints; otherwise an `account_id`, and a
   `category_id` unless the validation context sets `allow_uncategorized`. `interpret` passes no context, so a model
   `record` without a category always fails here.
3. `respond`: any other `ValueError`/`TypeError` in the call.

Downstream, `process_claim` asks the model once more on `provider_invalid_output` (commit `294b800`), then
`apply_claim` with no proposal raises `EvidenceError("provider_invalid_output")`, and `apply_deterministic_fallback`
records what the backend proves (`model_fallback`) or holds the event. Invalid model output never writes. The
benchmark calls `interpret` once per repeat and counts any `ProviderError` as a schema failure; it records usage but
not the reply.

### Provider documentation (checked 2026-11-09)

From [Structured Outputs](https://developers.openai.com/api/docs/guides/structured-outputs):

- "Supported `string` properties": `pattern` and `format` (`date-time`, `time`, `date`, `duration`, `email`,
  `hostname`, `ipv4`, `ipv6`, `uuid`). Numbers: `multipleOf`, `maximum`, `exclusiveMaximum`, `minimum`,
  `exclusiveMinimum`. Arrays: `minItems`, `maxItems`.
- `minLength` and `maxLength` are not in the supported list. They appear only under "For fine-tuned models, we
  additionally do not support the following", so the page does not say whether base models enforce them.
- "If you turn on Structured Outputs by supplying `strict: true` and call the API with an unsupported JSON Schema,
  you will receive an error." Our requests were accepted, so the schema passes the provider's check.
- Composition (`allOf`, `not`, `if`/`then`/`else`, `dependentRequired`) is not supported, and "the root level object
  of a schema must be an object, and not use `anyOf`". A conditional rule such as "a record needs a category" cannot
  be expressed in the current flat schema.
- "Handle edge cases": "In some cases, the model might not generate a valid response that matches the provided JSON
  schema", namely a refusal (a `refusal` content item, see the
  [Responses API reference](https://developers.openai.com/api/reference/resources/responses/methods/create)) or an
  incomplete response (`status: "incomplete"`, `incomplete_details.reason` such as `max_output_tokens`; the installed
  SDK `openai==3.19.2` also lists `content_filter`).

### Failure fingerprint

- Expected vs observed: schema completion ≥ 98%; observed 94.7% (en) and 93.0% (id), every failure
  `provider_invalid_output`; the safety gate passed.
- Input and sequence: one `interpret` call per case and repeat, 0.5 s apart, synthetic contexts without the
  production `classification` block.
- Affected cases: `mybca-id-expense`, `mybca-en-expense`, `bca-qris`, `integral-decimal`, `jago-stale-balance`; every
  other case was valid on all repeats. No case failed on all three repeats.
- Revision and configuration: `af8f95e`, prompt hashes `75167cf3…` (en) and `a055c71c…` (id).

### Offline evidence

**Token calibration.** For all 107 valid replies in both reports, `usage.output_tokens` equals the o200k_base token
count of the compact JSON of the recorded proposal plus exactly 10 (min 10, max 10). With effort `none` no reasoning
tokens are counted. So a failed reply's content length is `output_tokens − 10`, and candidate reply shapes can be
sized exactly with synthetic stand-ins (scripts kept outside the repository; they use only the synthetic fixtures).

**Reproduction.** A synthetic reply (`record`, expense, `account_id` = fixture account 3, `category_id` null, every
other field valid) passes `jsonschema` validation against the exact schema we send, yet
`OpenAINotificationProvider.interpret` behind a mock transport raises `provider_invalid_output`.

**Type gaps ruled out.** Pydantic strict JSON mode accepts integer confidence (0, 1), upper-case, unhyphenated and
braced UUIDs, and 160 multi-byte characters; it rejects 161 characters (`string_too_long`).

**Classification of the seven failed requests** (raw replies were not recorded; "content tokens" is
`output_tokens − 10`; a valid record of the same case is 126–133 content tokens, a valid empty `needs_review` 84–85):

| # | Prompt | Case | Repeat | Output tokens | Content tokens | Best-fitting shape | Rejecting check | Class |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | `-12-en` | `mybca-id-expense` | 2 | 123 | 113 | `record` with `category_id` (or `account_id`) null; sized 113 with "BCA payment" | `check_record_shape` | A (record shape) |
| 2 | `-12-en` | `mybca-en-expense` | 2 | 243 | 233 | about 100–150 tokens more than any valid reply | not recorded | B (unexplained long reply) |
| 3 | `-12-en` | `bca-qris` | 2 | 287 | 277 | 144 tokens more than the valid record (133) | not recorded | B |
| 4 | `-12` | `mybca-id-expense` | 1 | 279 | 269 | 139 tokens more than the valid record (130) | not recorded | B |
| 5 | `-12` | `jago-stale-balance` | 2 | 126 | 116 | valid record (132) with one UUID replaced by null = 116 exactly | `check_record_shape` | A |
| 6 | `-12` | `integral-decimal` | 1 | 123 | 113 | valid record (126) with `category_id` null = 110, plus description variance | `check_record_shape` | A |
| 7 | `-12` | `integral-decimal` | 2 | 123 | 113 | as #6 | `check_record_shape` | A |

Class A cannot tell a missing category from a missing account (both replace one UUID with null); both are the same
parse-time domain rule. For class B the reports cannot decide between an overlong string and a record-shape failure
carrying extra `mappings`: a 500–650-character description gives 254–292 content tokens on `bca-qris`, and one or
two mappings with three alternatives give 224 or 313 before removing a UUID.

### Hypothesis ledger

| ID | Mechanism | Supporting evidence | Conflicting evidence | Prediction that must hold | Discriminating experiment | Result |
| --- | --- | --- | --- | --- | --- | --- |
| H1 | Our parser applies a domain rule (record needs category/account) the sent schema allows to break: contract mismatch at the provider boundary | Offline reproduction; exact token fit for #5, fit within 3 tokens for #1, #6, #7; same cases record with a category on other repeats; rule only runs without `allow_uncategorized` | Raw replies not recorded | Instrumented rerun shows class A failures as `schema-valid` + `incomplete_record` rejections, none as `schema_mismatch` | Phase 3 rerun with failure capture | Supported for #1, #5, #6, #7 (strong, indirect) |
| H2 | Truncation at `max_output_tokens` | none | Max failed reply 287 tokens against a 2048 cap; effort `none` adds no reasoning tokens | An incomplete status would show ~2048 output tokens | Already decided by recorded usage | Rejected |
| H3 | Provider not enforcing the schema structurally (non-strict or unsupported keyword) | none | `strict: true`, all properties required, `additionalProperties: false`; requests accepted (unsupported schema would be an error); all 107 valid replies match the schema field for field | n/a | Already decided | Rejected for structure |
| H4 | Pydantic strict mode stricter than JSON Schema for types | none | Offline probe: ints, UUID variants accepted | n/a | Already decided | Rejected |
| H5 | `maxLength` not enforced by the provider for this model, so a long string fails `string_too_long` | `maxLength` not listed as supported; sizes of #2–#4 fit a 450–650-character description | Requests were accepted with `maxLength`; enforcement unknown | Rerun shows `schema_mismatch` on a string field with `string_too_long` | Phase 3 rerun | Open (candidate for class B) |
| H6 | Class B is a record-shape failure carrying `mappings` | Sizes fit one or two mappings; BCA cases have no unresolved names, so mappings would be unexpected | Prompt says mappings must be empty without unresolved names | Rerun shows class B as `incomplete_record` with `mappings` > 0 | Phase 3 rerun | Open |
| H7 | Refusal or `content_filter` incompleteness | none | Synthetic bank texts; a refusal is usually short | Rerun shows class `refusal` or `incomplete` | Phase 3 rerun | Open, low prior |
| H8 | Benchmark contexts lack the production `classification` block (`merchant_less`, `match_key`), so merchant-less BCA cases get fewer category hints and more class A replies | The prompt's merchant-less instruction refers to `classification.merchant_less`; all class A cases are merchant-less or unfamiliar-merchant texts | Not measured | Adding the block lowers class A frequency | Follow-up (D8), not in this change | Open, contributing condition |

## Goals / Non-Goals

**Goals:** make the provider boundary reject only what violates the contract we send; keep every domain rule and
the no-write property; make each remaining invalid output diagnosable without content; measure again with evidence.

**Non-goals:** see proposal.md. Design-level: no change to fallback semantics, review reasons, error codes, the
retry count for structural failures, or the request until phase 4 evidence exists.

## Decisions

**D1 Separate the structural contract from the record-shape rule.** `Interpretation` validates only what the sent
schema states (types, enums, nullability, lengths, counts, ranges). The rule in the former `check_record_shape`
model validator becomes `record_shape_error(proposal, *, allow_uncategorized)`, which returns a diagnostic reason
(`incomplete_record`, `observed_account_required`, `category_required`, `movement_endpoints_required`,
`movement_endpoints_identical`) or `None`. `validate_interpretation` runs it first and raises the existing
`EvidenceError("model_uncertain")`, so no new error code reaches the database, the API or the message catalogues
(every `EvidenceError` code must be an `ErrorCode` with server and web messages). The reason is reported only by the
benchmark. `apply_interpretation`, `resolve_record` and the seen-twice path pass `allow_uncategorized` only for the
`deterministic` and `deterministic_fallback` sources (`DETERMINISTIC_SOURCES`), so "only the deterministic path may
record without a category" is unchanged. The memory and deterministic builders go through `backend_interpretation`,
which raises `ValueError` if backend code ever builds an incomplete record: that is a programming error, the same
exception class the pydantic validator raised before. The benchmark's `trusted_decision`, the dry run and
`mapping_benchmark` reach the rule through `validate_interpretation`. Rejected: expressing the rule in the schema
(strict mode has no `if`/`then`, and the root cannot be `anyOf`; see D7); keeping the validator and catching its
error type in `interpret` (keeps two meanings in one parse step and the mismatch with the sent schema); five new
error codes (an API and catalogue change for a diagnostic need).

**D2 An incomplete model record is model uncertainty, not a provider failure.** With D1 the provider returns the
proposal; `apply_claim` reaches `apply_interpretation`, which raises the domain code, and the existing
`apply_deterministic_fallback` records with `model_fallback` or holds the event. The retry in `process_claim` keeps
firing only for `provider_invalid_output`, so no second request is made: asking again until the model picks a
category would suppress the uncertainty the prompt tells it to keep, and integral-decimal repeated it on two of
three repeats. Rejected: retrying (pays for a forced answer); accepting the model's category-less record with
`uncertain_category` (changes the approved "deterministic path only" rule; offered as a follow-up because it would
keep the model's description and merchant).

**D3 (default) Content-free failure class on `ProviderError`.** `ProviderError` gains an optional `diagnosis`
(`OutputDiagnosis`: failure `refusal` | `incomplete` | `empty_output` | `json_decode` | `schema_mismatch` |
`malformed_response`, `incomplete_reason`, a tuple of `field_path:error_type` built from pydantic `loc` and `type`
only, output tokens). A path segment that is not a schema property name or a list position is written `?`, because
an extra key's name is model-controlled text. `respond` reads a refusal item and `incomplete_details.reason` before
`output_text`, and never parses partial text. One shared `parse_output` serves `interpret`, `normalize_purpose`,
`normalize_capture` and `suggest_history` (the separate `history_suggestions.parse_batch` is removed).
`process_claim` logs one warning line per invalid reply with the event id, model, effort, prompt version and the
diagnosis, and nothing else. Error codes and stored data do not change. Rejected: new error codes per class
(changes stored values, message catalogues and clients for a diagnostic need); storing the diagnosis on the event
(a schema change without a present consumer).

**D4 (default) Reply capture only in the benchmark.** The provider constructor takes
`capture_invalid_output: bool = False`; only `notification_benchmark.run_live` sets it. When set, the provider keeps
the last invalid reply's text (first 4,000 characters) next to the diagnosis; the benchmark row stores the
diagnosis, a shape summary (outcome, direction, null reference fields, string lengths, mappings count; computed by
decoding the JSON when possible) and the raw synthetic text. The spec already lets synthetic reports retain raw
proposals. Production never sets the flag, and a test asserts the worker's provider has it off. Rejected: a
production sampling log of replies (personal financial data, against the "no content in logs" rule).

**D5 (default) Benchmark scoring.** `schema` stays "a well-formed proposal was returned", now meaning the sent
schema. Domain-incomplete records count as validation rejections (`model_uncertain`), carry their reason in the
row's `incomplete_record`, and add to a new `incomplete_records` count. They remain wrong for classification,
references and the validated outcome. Each failed request carries `invalid_output` (diagnosis, shape, synthetic
reply), and the summary counts failures by class in `invalid_output_failures`. The 98%
schema threshold is unchanged. Rejected: counting incomplete records as schema failures (re-creates the conflation
that hid the cause); lowering the threshold.

**D6 (default) The gate stays single-attempt.** The benchmark keeps one request per repeat and does not replay the
production retry, so its schema rate is a lower bound for production. Rejected: emulating the retry (hides the
first-attempt rate and raises cost).

**D7 (default) No request or prompt change before the rerun.** Prompts `-12` and `-12-en`, the schema,
`strict: true`, `max_output_tokens` 2048, model and effort stay as they are, so the rerun changes one variable: how
we classify replies. Rejected now: raising `max_output_tokens` (H2 rejected); effort `low` (production parameter
change, cost, no evidence it helps); a discriminated union under a root property, for example
`{"result": anyOf[expense_or_income_record, movement_record, review, ignored]}` (supported nested, but it changes
the response shape for the provider, dry run, benchmark and both prompts, forces a category on records, and needs a
new prompt version and baseline; reconsider only if incomplete records stay frequent after D8).

**D8 (default) Fixture fidelity is a follow-up.** Adding the production `classification` block to benchmark
contexts would change the fixture version and the baseline at the same time as D1–D5. It is recorded as a follow-up
change to run after this one.

**D9 Conditional fix for class B, decided on phase 3 evidence.** The owner approves the branch before any code:

- `string_too_long` on a field (H5): the provider is not enforcing `maxLength` for this model. Default fix: send
  the bound with a supported keyword (`pattern` such as `^[\s\S]{0,160}$`) for each bounded string field, keep the
  pydantic bound as the backend check, bump the schema identity in the report, and rerun both prompts. Rejected:
  truncating replies (evidence must stay verbatim, and a cut description is an invented value); raising the bounds.
- `incomplete_record` with mappings (H6): already handled by D1/D2; no further code.
- `refusal` or `incomplete` (H7): stays `provider_invalid_output` → retry once → deterministic fallback. Report it;
  no fix in this change.
- Class B does not recur in the instrumented rerun: no phase 4 code. The failures stay observable through D3/D4.

**D10 (default) Decision rule for `LLM_ENGLISH_OUTPUT_ENABLED`.** Recommend enabling only when, on one revision
and the production configuration (`gpt-6-luna`, effort `none`): the English development and held-out runs both
pass the safety gate, the quality gate (schema ≥ 98%, unambiguous classification ≥ 90%, description rubric ≥ 90%)
and the deterministic routes; English record recall is no more than 5 percentage points below the Indonesian run of
the same partition and revision; and no class B failure is unexplained. The agent only recommends; the owner flips
the flag through the release path. If Indonesian fails where English passes, the production prompt gets its own
follow-up; it does not block English.

## Evaluation plan

All runs are synthetic, need the owner's explicit approval before each one, use the owner-supplied credential (the
agent never reads `.env`), and run with `--live --model gpt-6-luna --reasoning-efforts none --repeats 3`. The default
cap (`DEFAULT_MAX_ESTIMATED_USD` 0.25) is below the upper bounds, so each run passes `--max-estimated-usd 0.31`.
Estimates use the benchmark constants (input $0.20 and output $1.20 per million tokens, upper bound = input bytes as
tokens + 2,048 output tokens per request); the projection uses the observed 3,200 input and 150 output tokens per
request.

| Run | Prompt | Partition | Model requests | Upper bound | Projected | Condition |
| --- | --- | --- | --- | --- | --- | --- |
| R1 | `-12` | development | 57 | $0.3014 | $0.047 | after phases 1–2 |
| R2 | `-12-en` | development | 57 | $0.3033 | $0.047 | after phases 1–2 |
| R3 | `-12` and/or `-12-en` | development | 57 each | $0.30 each | $0.047 each | only if D9 adds a request change |
| R4 | `-12` | held_out | 51 | $0.2719 | $0.042 | its development run passed |
| R5 | `-12-en` | held_out | 51 | $0.2737 | $0.042 | its development run passed |

Worst case (R1–R5 with R3 for both): upper bound $1.76, projected about $0.27. Without R3: upper bound $1.15,
projected about $0.18. The 2026-11-08 runs cost $0.047 and $0.046 observed. Each report goes to
`cash-flow-tracker-prototypes/research/evidence/` with the revision and a note in the change's `verification.md`.

## Risks / Trade-offs

- [The class A attribution is indirect (token arithmetic, not recorded replies)] → R1/R2 record replies; if class A
  shows a different shape, D1 still stands as a contract fix and the ledger is updated before more code.
- [D5 can raise the schema rate without the model getting better] → incomplete records are reported separately and
  stay wrong in every correctness metric; the proposal and report say so.
- [Removing the parse-time validator could let an incomplete deterministic interpretation through] → the
  deterministic builders call the same shape check; tests cover each builder and `apply_interpretation` for every
  source.
- [The `pattern` route in D9 may be rejected or slow for some models] → it only ships after R3 shows accepted
  requests and no schema failures.
- [Logging the diagnosis could leak content through field paths] → paths use schema property names and list
  indexes only; a test feeds a reply with a sentinel value and asserts the log line lacks it.

## Migration Plan

No data migration. The change ships through the normal release path. Rollback is a revert: error codes, stored data
and the request are unchanged in phases 1–2, so old and new workers handle the same events the same way apart from
the skipped retry.
