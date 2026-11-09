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

## Phases 3–5 (paid, gated)

Not run. Each run needs the owner's explicit approval of its spend first.
