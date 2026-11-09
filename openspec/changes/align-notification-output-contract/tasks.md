## 1. Align the output contract with the sent schema (D1, D2)

- [x] 1.1 Add offline regression tests that fail on `a92b66a` for the expected reason: a synthetic reply valid for `response_schema()` (checked with `jsonschema` against the sent schema) with `outcome` `record` and `category_id` null, and one with `account_id` null, both served through `httpx2.MockTransport`; evidence: the tests fail with `provider_invalid_output` before 1.2 and pass after
- [x] 1.2 Move the record-shape rule out of the `Interpretation` model validator into a backend check returning the domain codes in D1; `validate_interpretation` runs it first; verify with unit tests per code (blank description, missing direction, empty amount evidence, missing account, missing category, missing movement endpoint, identical endpoints) and a negative test that a `needs_review` or `ignored` reply with null references passes
- [x] 1.3 Pass `allow_uncategorized` only for the `deterministic` and `deterministic_fallback` sources in `apply_interpretation`, and make `deterministic_interpretation` and `memory_interpretation` call the same check; verify that an AI-sourced category-less record is rejected, a deterministic one is accepted, and the existing `test_notification_interpretation.py` and `test_notification_application*` suites pass
- [x] 1.4 Database tests (disposable migrated PostgreSQL on port 9830–9849, container `cft-aiout-pg`): a model `record` without a category leads to one `interpret` call and a `model_fallback` row with `review_status = needs_review`; without an account no field comes from the reply and the deterministic path records or holds; a movement with one endpoint never writes from the reply; an unresolved-name record without an account goes to mapping confirmation or review; `test_unusable_model_answer_is_asked_again_once` still passes; report any skipped tests
- [x] 1.5 Check every other caller of `Interpretation.model_validate_json` (`notification_dry_run.py`, `evaluation/mapping_benchmark.py`, `evaluation/mobile_smoke.py`) and verify with `rg` plus their tests that none relied on the parse-time rule
- Expected Result: a schema-valid incomplete record is a domain rejection with the deterministic fallback, never `provider_invalid_output`, and never written from the reply.

## 2. Diagnose invalid replies without content (D3–D6)

- [x] 2.1 Add the content-free `diagnosis` to `ProviderError` and set it in `respond` and the shared parse helper for refusal, incomplete (`max_output_tokens`, `content_filter`), empty output, JSON decode and schema mismatch; verify with one mock-transport test per class, including a refusal item with no output text and an `incomplete` status with partial text that is never parsed
- [x] 2.2 Log one line per `provider_invalid_output` in `process_claim` with event id, model, effort, prompt version and diagnosis; verify with a `caplog` test whose reply carries a sentinel string in `description`, `merchant` and the notification, asserting the line holds `description:string_too_long` and none of the sentinels, and that the stored `error_code` is `provider_invalid_output`
- [x] 2.3 Add `capture_invalid_output` (default off) to the provider and enable it only in `run_live`; verify with a test that the worker's provider factory leaves it off and that the captured text is cut at 4,000 characters
- [x] 2.4 Extend the benchmark row and summary: diagnosis, shape summary, raw synthetic reply on failure, `incomplete_records` count and validation rejection codes; verify with `test_notification_benchmark.py` cases built from the class A shape (schema-valid, rejected `category_required`, counted in `incomplete_records`, wrong for classification) and a class B stand-in (161-character description → `schema_mismatch`, `description:string_too_long`, counted as a failed request)
- [x] 2.5 Run the backend suite as CI does (`PYTHONPATH=backend backend/.venv/bin/python -m pytest backend/tests` with the disposable database), `openspec validate --all --strict` and `git diff --check`; record results and skips in `verification.md`; remove the test container by exact name
- Expected Result: every invalid reply names its class in tests, logs and synthetic reports; production logs contain no reply or notification content.

## 3. Instrumented development reruns (gated: owner approval required for each paid run)

- [ ] 3.1 Ask the owner to approve R1 and R2 with the estimates in design.md (upper bound $0.3014 and $0.3033, projected about $0.047 each); evidence: the owner's approval quoted with its date in `verification.md`
- [ ] 3.2 Run R1 (`--output-language id`) and R2 (`--output-language en`) on the development partition with `--live --model gpt-6-luna --reasoning-efforts none --repeats 3 --max-estimated-usd 0.31` and the owner-supplied credential; evidence: both reports saved under `cash-flow-tracker-prototypes/research/evidence/` with revision, clean tree, prompt and fixture hashes, observed cost
- [ ] 3.3 Classify every failed request from the captured diagnosis and update the hypothesis ledger in design.md (H1, H5, H6, H7 confirmed or rejected); evidence: the updated ledger and a per-failure table in `verification.md`
- [ ] 3.4 Present the class B result and the matching D9 branch to the owner and record the decision; evidence: the owner's choice and date in design.md
- Expected Result: the cause of each remaining failure is known from recorded synthetic replies, and the owner has chosen the D9 branch.

## 4. Conditional class B fix (only if 3.4 selects a code change)

- [ ] 4.1 Implement the selected D9 branch (for H5: `pattern`-bounded strings in the sent schema with the pydantic bounds kept); verify with offline tests that the sent schema contains the pattern for each bounded field, that a reply violating it is still `schema_mismatch`, and that `strict_json_schema` output passes `jsonschema` meta-validation
- [ ] 4.2 With owner approval for R3 (about $0.30 upper bound per prompt), rerun the development partition for each affected prompt; evidence: reports with no `schema_mismatch` on the fixed fields and the request accepted (no `provider_request_rejected`)
- Expected Result: the class B mechanism no longer produces invalid output, proven on the development partition.

## 5. Held-out confirmation and the English decision (gated)

- [ ] 5.1 For each prompt whose development run passed the safety and quality gates, ask the owner to approve its held-out run (R4 `-12` $0.2719, R5 `-12-en` $0.2737 upper bound, about $0.042 each projected) and run it; evidence: approval, reports and observed cost in `verification.md`
- [ ] 5.2 Apply the D10 rule to the English results and write the recommendation (enable or keep `false`, with each criterion's value); evidence: the recommendation table in `verification.md` and the research note updated in `cash-flow-tracker-prototypes/research/`
- Expected Result: the owner has a pass/fail recommendation for `LLM_ENGLISH_OUTPUT_ENABLED` backed by development and held-out evidence.

## 6. Integration and hand-off

- [ ] 6.1 Review the final diff for content in logs, secrets, dead code (the removed model validator and any obsolete test) and unrequested changes; update the LLM boundary notes in `AGENTS.md` only if a stated fact changed; evidence: review notes in `verification.md`
- [ ] 6.2 After the owner accepts, push `fix/notification-ai-invalid-output`; when the owner says "open", create the pull request with `gh pr create` and report its URL; never merge it
- Expected Result: one reviewed branch with specs and code together, awaiting the owner's merge.

## Workflow follow-up

- Archive the change after the owner accepts and the branch merges.
- Follow-ups recorded in design.md: benchmark `classification` context fidelity (D8); keeping the model's description and merchant on a category-less record (D2 alternative); the English `needs_review` empty-description recall; the rubric's proper-name false positive.
- The owner decides and performs the `LLM_ENGLISH_OUTPUT_ENABLED` change through the release path; track it in `PROJECT_STATE.md`.
