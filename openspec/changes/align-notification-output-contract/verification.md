# Verification: align-notification-output-contract

## Approval

Plan `095ee4c` approved on 2026-11-09 under the owner's standing complete-all authorization, with D1–D10 accepted.
The approval covers phases 1–2 (offline). No paid run has been approved or made.

## Phases 1–2 (offline), 2026-11-09

Tested state: branch `fix/notification-ai-invalid-output` from `a92b66a`, the phase 1–2 working tree committed
with this file. Python 3.12 virtual environment from `uv venv -p 3.12` with `backend/requirements-dev.txt`.
Disposable database `cft-aiout-pg` (`postgres:16`, loopback port 9831) migrated V1 → V54 with `flyway/flyway:10`
(`Schema version: 54`), recreated fresh before the gate run and removed afterwards.

| Check | Command | Result |
| --- | --- | --- |
| Backend suite, baseline at `095ee4c` | `pytest tests/ -ra` with `TEST_DATABASE_URL`, `SKIP_TEST_MIGRATIONS=1`, `REQUIRE_TEST_DATABASE=1` | 2203 passed, 0 skipped |
| Backend suite, after phases 1–2 (fresh database) | same | 2234 passed, 0 skipped, 0 failed |
| Regression on the base revision | new class A provider tests run against `a92b66a` application code | 2 failed with `provider_invalid_output` (expected reason) |
| OpenSpec | `openspec validate --all --strict` | 85 passed, 0 failed |
| Whitespace | `git diff --check` | clean |
| Frontend | not run | no frontend file changed |

### Evidence by task

- 1.1: `test_schema_valid_incomplete_record_is_not_an_invalid_output` (category null, account null) asserts the
  sent schema allows null for the field and that `interpret` returns the record. It fails on `a92b66a` and passes now.
- 1.2: `test_incomplete_record_is_schema_valid_and_rejected_as_model_uncertainty` covers blank description, missing
  direction, empty amount evidence, missing account, missing category, missing movement endpoint and identical
  endpoints. Each is parsed, given its reason, and rejected with `model_uncertain`.
  `test_review_and_ignored_replies_with_empty_fields_are_complete` is the negative case.
- 1.3: `test_only_backend_sources_may_record_without_a_category`. The existing
  `test_unusable_model_answer_without_category_records_uncategorised_and_flagged` and deterministic suites pass
  unchanged.
- 1.4 (database): `test_incomplete_model_record_falls_back_without_asking_again` (category null and account null:
  one request, deterministic fallback row with `model_fallback`, no model description),
  `test_model_movement_with_one_endpoint_is_never_written_from_the_reply` (one request, the deterministic movement
  is recorded from the text), `test_incomplete_record_for_an_unresolved_name_does_not_write_or_learn` (no row, no
  alias). `test_unusable_model_answer_is_asked_again_once` still passes.
- 1.5: `rg "Interpretation.model_validate_json|validate_interpretation|resolve_record"`. The dry run and
  `mapping_benchmark` reach the rule through `validate_interpretation`. The seen-twice path passes the source's
  allowance. `mobile_smoke` builds a complete record. The memory and deterministic builders use
  `backend_interpretation`.
- 2.1: `test_each_invalid_output_class_is_diagnosed` (incomplete `max_output_tokens` and `content_filter`, refusal,
  empty output, JSON decode, malformed response), `test_schema_mismatch_names_fields_and_error_types_without_values`
  (an extra key is recorded as `?`, no sentinel value appears), `test_incomplete_partial_text_is_never_parsed`, and
  `test_sibling_calls_share_the_diagnosis`.
- 2.2: `test_structurally_invalid_reply_is_asked_again_and_logged_without_content` asserts one log line with
  `failure=schema_mismatch`, `fields=description:string_too_long` and `output_tokens=120`, and no sentinel,
  notification text or amount in the captured log. `test_two_structurally_invalid_replies_keep_the_existing_fallback`
  asserts the unchanged fallback.
- 2.3: `test_reply_text_is_kept_only_when_capture_is_enabled` (default off, 4,000-character cut, refusal text,
  cleared on success) and `test_only_the_synthetic_benchmark_enables_reply_capture`.
- 2.4: `test_schema_valid_record_without_category_is_scored_as_incomplete_not_as_schema_failure` and
  `test_invalid_reply_shape_and_synthetic_text_are_recorded`.

### Deviation from the planned design

D1 first named five new domain error codes. Every `EvidenceError` code must be an `ErrorCode` with server and web
messages, so new codes would have changed the API and the catalogues. The implementation raises the existing
`model_uncertain` and keeps the specific reason as a benchmark diagnostic only. design.md and the spec deltas were
updated to match.

## Phases 3–5 (paid), 2026-11-09

The owner approved the development and held-out runs. The coordinator ran all four from `6cf37f2`
(`6cf37f23a543a818ad38e74ce6440622efb3b4db`, reported `dirty: false`) with `--live --model gpt-6-luna
--reasoning-efforts none --repeats 3 --max-estimated-usd 0.31` and deleted the credential copy after each pair. Every
`.stderr` file is empty. Reports (owner-local, synthetic):
`cash-flow-tracker-prototypes/research/evidence/output-contract/bench-{id,en}-{dev,heldout}.json`.

| Run | Prompt | Partition | Requests | Schema | Record recall | Unambiguous classification | Movement P / R | Incomplete records | Rejections | Safety | Quality | Upper bound | Observed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| R1 | `-12` | development | 57 | 98.2% (56/57) | 98.0% | 100% | 100% / 100% | 3 | 3 | pass | pass | $0.3014 | $0.0458 |
| R2 | `-12-en` | development | 57 | 100% | 86.3% | 100% | 75% / 100% | 1 | 4 | pass | pass | $0.3033 | $0.0463 |
| R4 | `-12` | held_out | 51 | 100% | 100% | 66.7% | 100% / 33.3% | 1 | 1 | pass | **fail** | $0.2719 | $0.0421 |
| R5 | `-12-en` | held_out | 51 | 100% | 92.2% | 66.7% | 100% / 33.3% | 12 | 12 | pass | **fail** | $0.2737 | $0.0420 |

Observed total $0.1763 (token-priced with the benchmark constants, not account billing). Deterministic routes and the
safety gate pass on all four runs. For comparison, 2026-11-08 at `af8f95e`: schema 93.0% (`-12`) and 94.7% (`-12-en`).

### Task 3.3: every failure, incomplete record and rejection

| Run | Case | Repeat | Field | Value | Meaning |
| --- | --- | --- | --- | --- | --- |
| R1 | `mybca-id-expense` | 1 | `invalid_output` | `json_decode`, 187 output tokens, 724 characters | The same 362-character `needs_review` object written twice with no separator; status `completed`. Class B, H9 |
| R1 | `mybca-id-expense` | 2, 3 | `incomplete_record` | `category_required` | `record` on BCA without a category (confidence 0.6, 0.55). Class A, now a `model_uncertain` rejection |
| R1 | `jago-stale-balance` | 1 | `incomplete_record` | `category_required` | Class A |
| R2 | `jago-unknown-pocket` | 1–3 | rejection | `uncertain_pocket_mapping` | `record` movement with a mapping for the unregistered "Rahasia" pocket (confidence 0.35–0.63); the trusted decision holds it. Expected `needs_review`; this is why movement precision is 75%. Not an output-contract issue |
| R2 | `jago-default-payment` | 2 | `incomplete_record` | `category_required` | Class A |
| R4 | `sequence-mybca-debit` | 2 | `incomplete_record` | `category_required` | Class A |
| R5 | `gopay-bank-out` | 1–3 | `incomplete_record` | `category_required` | Self-transfer to BCA left without a category |
| R5 | `movement-29`, `-30`, `-history-omitted` (×2), `-out-of-order` (×2), `-equal-unrelated` (×2) | | `incomplete_record` | `category_required` | The same self-transfer text in the movement fixtures |
| R5 | `sequence-mybca-debit` | 2 | `incomplete_record` | `category_required` | Class A |

Class A recurred 17 times and none is a provider failure any more. In production each is a `model_uncertain`
rejection followed by the deterministic fallback, with no second request. Class B recurred once (0.46% of 216
requests). No `schema_mismatch`, `string_too_long`, `refusal` or `incomplete` occurred, so the request needs no
change. Why strict mode let the H9 reply through: each object conforms to the schema; the provider ended the
response as `completed` after repeating the object instead of stopping at the first closing brace. The OpenAI
Structured Outputs guide documents refusals and incomplete responses as the cases that may not match the schema and
does not document this one, so this is observed behaviour, not a documented guarantee.

### Task 3.4: D9 branch

No phase 4 code (design.md, "D9 outcome"). The repeated-object reply stays `provider_invalid_output`, the production
retry covers it, and nothing needs a schema or request change.

### Held-out classification failure (separate follow-up, not caused by this change)

- The unambiguous-classification set on held-out is `jago-coffee`, `jago-learning` and `gopay-bank-out` (9 rows).
  `jago-coffee` and `jago-learning` pass every repeat in both languages. `gopay-bank-out` ("Rp500.000 udah dikirim
  ke BCA Mira Langit.", the owner's own name) fails every repeat: Indonesian picks "Kebutuhan Rumah" or "Jajan
  Favorit", English leaves the category empty. The fixture expects "Internal Movement". 6/9 = 66.7%.
- Movement recall 33.3% comes from the `movement-*` fixtures expecting the model to propose
  `candidate_transaction_id`. The model never does in either language.
- Both scores measure model fields that production code decides itself: `resolve_record` replaces the category of a
  self-transfer with the movement category, and pairing is done by `movement_pairing`. The model's
  `candidate_transaction_id` is only validated as a reference.
- The same failure predates this change: the archived 2026-10-30 held-out `none` run
  (`add-ai-notification-processing/evaluation/held-out-none-v4.json`, prompt version 4) scored exactly 66.67%
  unambiguous classification with `gopay-bank-out` failing all three repeats, and 25% movement recall.
- This change does not alter the prompt, request or the model's proposal, and a failed request was already counted
  wrong for classification, so the classification and movement metrics are unaffected by it. Held-out had never run
  on prompt version 12 before today.
- Follow-up: decide whether the benchmark should score the trusted decision for backend-owned fields (self-transfer
  category, movement pairing) or the prompt should name the self-transfer category; also the 12 English
  category-less self-transfers, which raise the review load through `model_fallback`, and the
  `jago-unknown-pocket` expectation against the mapping-confirmation flow.

### Task 5.2: D10 decision

| Criterion | Required | English result | Pass |
| --- | --- | --- | --- |
| Safety gate, development and held-out | pass | pass, pass | yes |
| Quality gate, development | pass | pass | yes |
| Quality gate, held-out | pass | fail (classification 66.7%) | no |
| Deterministic routes | pass | pass, pass | yes |
| Record recall gap to Indonesian, development | ≤ 5 points | 11.7 (86.3% against 98.0%) | no |
| Record recall gap to Indonesian, held-out | ≤ 5 points | 7.8 (92.2% against 100%) | no |
| Unexplained class B | none | none in English | yes |

Recommendation: keep `LLM_ENGLISH_OUTPUT_ENABLED=false`. The Indonesian production prompt also fails the held-out
quality gate, for the reason above; that is the separate follow-up and does not change production now.

## Final review (task 6.1)

- Logs: the only new log line is `log_invalid_output`, holding the event id, model, effort, prompt version and the
  content-free diagnosis. A database test checks that sentinel reply values, the notification text and the amount
  stay out of it. Reply text is captured only when `capture_invalid_output=True`, which only the benchmark sets.
- Secrets: none in the diff. Tests use `synthetic-test-credential`.
- Dead code: the `check_record_shape` model validator and `history_suggestions.parse_batch` are removed, with no
  remaining callers. The test that expected an empty description to fail parsing moved to the record-shape tests.
  pyflakes reports four unused names in the changed files (`main_endpoint`, `json`, `timezone`, a test local
  `second`). All four predate this change and are left alone.
- `AGENTS.md`: its LLM boundary list names files, not the parse rule, so no stated fact changed.
- Tasks 4.1 and 4.2 stay unchecked as not applicable (D9 outcome).
