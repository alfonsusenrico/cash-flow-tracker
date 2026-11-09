## Approval

Plan `095ee4c` approved on 2026-11-09 under the owner's standing complete-all authorization, with every default
(D1–D10) accepted. D2 was accepted as recommended. The approval covers the offline phases 1–2. Each paid benchmark run
(tasks 3.1 onwards) still needs the owner's explicit approval of its spend.

## Why

The 2026-11-08 synthetic benchmark (`gpt-6-luna`, effort `none`, development partition, 3 repeats) failed the
quality gate for both prompts because schema completion was below 98%: `notification-interpretation-12` 93.0%
(4 of 57 requests) and `notification-interpretation-12-en` 94.7% (3 of 57), all `provider_invalid_output`.
Investigation without new paid calls shows that at least four of the seven failures are replies the provider
produced correctly against the strict schema we send, which our parser then rejects with a domain rule (a
`record` without a category or observed account). The rejection is reported as an invalid provider output, triggers
a second paid request in production, and hides the real outcome. The other three failures are longer replies whose
cause the reports did not record. This blocks `LLM_ENGLISH_OUTPUT_ENABLED` and also affects the production
Indonesian prompt.

## What Changes

- The provider boundary accepts every reply that satisfies the JSON schema sent to OpenAI. Domain rules on a
  `record` (category, observed account, movement endpoints, non-empty description) move to backend validation, where
  a violation is a typed domain outcome instead of `provider_invalid_output`.
- A model `record` that lacks a required reference is handled as model uncertainty: no second model request, no
  write from the model reply, and the existing deterministic fallback records what the backend can prove, flagged
  `needs_review` with the reason `model_fallback`, or holds the event.
- `provider_invalid_output` carries a content-free failure class (refusal, incomplete with its reason, empty
  output, undecodable JSON, schema mismatch with field names and error types only). Production logs only that class;
  the stored and API-visible error code stays `provider_invalid_output`.
- The synthetic benchmark records the failing reply's shape and its raw synthetic text, counts domain-incomplete
  records separately from schema failures, and keeps them out of correct scores.
- Offline regression tests reproduce each failure class from synthetic replies.
- A gated evaluation plan: instrumented development reruns for both prompts, a conditional evidence-specific fix for
  the still-unexplained long replies, held-out reruns, and the decision rule for `LLM_ENGLISH_OUTPUT_ENABLED`.

No change to the production prompts, the request (schema, `strict`, `max_output_tokens`, reasoning effort, model),
the database schema or stored data in phases 1 and 2. A request change is only possible in the conditional phase 4
and needs the owner's approval of the evidence first.

## Expected Outcome

- The owner sees a benchmark report in which every `provider_invalid_output` names its failure class and, for the
  synthetic cases, the reply that caused it.
- A schema-valid category-less `record` no longer counts as a provider failure, costs no second request in
  production, and still never writes from the model reply.
- After the gated reruns the owner has the evidence to decide whether to enable English AI output.

## Non-goals

- The English `needs_review` cases with an empty description (`integral-decimal`, `mybca-id-expense` in English;
  `archived-mapping`, `mybca-en-expense` in Indonesian): the prompt asks for empty descriptions on `needs_review`,
  and those events already go through the deterministic fallback. Recall work is a follow-up.
- The description rubric's false positive on `jago-custom` (Indonesian pocket names read as function words).
- Adding the production `classification` context block to the benchmark fixtures (recorded as a fidelity follow-up,
  design D8).
- A discriminated-union output schema, a prompt rewrite, a model change or a reasoning-effort change.
- Production: no deploy, flag change or production access. Local verification is offline and synthetic; paid
  benchmark runs need the owner's explicit approval each time; enabling `LLM_ENGLISH_OUTPUT_ENABLED` is the
  owner's decision through the approved release path.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `ai-notification-processing`: the structured-output contract between the schema we send and the validation we
  apply; content-free invalid-output diagnostics; the synthetic benchmark's failure-shape capture and
  incomplete-record scoring.
- `notification-record-and-flag`: a model `record` that lacks a required reference is model uncertainty handled by
  the deterministic fallback without a second request.

The project capability list (`llm` included) does not change. Applicable standards: `genai-engineering`
(structured output: validate, bounded retry, fail closed; no content in logs; evals before shipping) and
`backend-engineering` (explicit error handling).

## Impact

- Code: `backend/app/services/notification_interpretation.py` (record-shape rule leaves the parse-time model
  validator), `openai_notification_provider.py` (failure class), `notification_application.py` (deterministic
  builders and `apply_interpretation` call the shape check with the source's allowance), `notification_dry_run.py`,
  `backend/evaluation/notification_benchmark.py` (failure capture, metrics), `backend/evaluation/mapping_benchmark.py`
  and `mobile_smoke.py` if they rely on the parse-time check; tests in `backend/tests/`.
- API: unchanged. `error_code` values and messages stay the same; a domain-incomplete record surfaces as the existing
  `model_fallback` review reason or the existing hold codes.
- Data: no migration. No new stored field.
- Cost: removes one paid retry per domain-incomplete record in production. The gated reruns are estimated in
  design.md (upper bound about $0.30 per language per development run).
