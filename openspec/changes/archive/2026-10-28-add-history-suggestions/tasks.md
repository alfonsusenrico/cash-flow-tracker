## 1. Schema and settings

- [x] 1.1 V31 `suggestion_runs`, `transaction_suggestions`, feedback `history_suggestion_id` and the `accepted_suggestion` kind, with the identical startup mirror run last by `init_db.py` (design D1).
  - Evidence: Flyway V1→V31 on a fresh PostgreSQL; `test_v31_matches_mirror_and_is_idempotent` (mirror identical, applied twice, kind constraint widened, append-only trigger still rejects updates); partial unique index exercised in `test_one_active_run_per_owner_and_interrupted_runs_expire`.
- [x] 1.2 `HISTORY_SUGGESTION_MAX_ROWS` and `HISTORY_SUGGESTION_BATCH_SIZE` with bounds; invalid values disable runs (D9).
  - Evidence: `test_invalid_configuration_disables_runs` (503 `suggestion_configuration_invalid`); defaults in `.env.example`, Compose, materialiser and deploy workflow.

Expected Result: the database can hold runs, suggestions and accepted-suggestion examples, and cost bounds are configurable.

## 2. Generation

- [x] 2.1 Eligibility, estimate and confirmed start with one active run per owner and lease expiry (D2, D3, D6).
  - Evidence: `test_eligibility_excludes_owner_movement_fee_system_and_out_of_window_rows` (estimate creates nothing, one eligible row of seven), `test_estimate_counts_model_calls_and_respects_max_rows` (5 eligible, 3 selected, 2 calls, model not called), `test_one_active_run_per_owner_and_interrupted_runs_expire` (409, `run_interrupted`, new run allowed), `test_invalid_windows_are_rejected` (5 bodies, 422).
- [x] 2.2 Memory first, then repayment and café rules, skipping equal and rejected proposals (D4).
  - Evidence: `test_memory_first_rule_suggestion_skips_the_model`, `test_signature_memory_and_repayment_and_cafe_rules` (signature key from a real ingested myBCA row, repayment offset, café pillar, reasons without notification text), `test_rules_read_the_recording_notification_text` ("Pembayaran gaji" is not a repayment).
- [x] 2.3 Model batches with a strict schema and prompt, redacted input, validation and the degraded path (D5, D6).
  - Evidence: `test_provider_batches_are_validated_and_stored` (2 calls of 2+1 rows, invalid category dropped, owner name redacted, logs without names), `test_low_confidence_and_unchanged_proposals_are_skipped`, `test_provider_failure_completes_with_memory_suggestions_and_error_code`, `test_ai_disabled_records_the_reason_and_keeps_memory_suggestions`, `test_history_suggestions_use_their_own_strict_schema_and_prompt`, `test_history_suggestions_reject_invalid_output`.

Expected Result: an owner-confirmed run stores memory, rule and model suggestions within the cost bounds, and degrades to memory and rules without the model.

## 3. Decisions

- [x] 3.1 Listing grouped by proposed merchant and category, and supersession of stale suggestions (D7, D8).
  - Evidence: `two_kedai_suggestions` group assertions (count, total, before values), `test_later_edit_supersedes_the_suggestion`, `test_review_accept_without_change_does_not_supersede`.
- [x] 3.2 Accept through the owner-edit hooks; reject remembered; owner scoping (D8).
  - Evidence: `test_accept_applies_through_owner_edit_hooks` (row values, owner classifier, reviewed, 2 `accepted_suggestion` feedback rows with suggestion ids, owner memory, idempotent repeat), `test_accept_one_row_of_a_group`, `test_reject_is_remembered_by_later_runs` (no feedback, rerun reports 2 rejected before), `test_other_owners_suggestions_are_not_found` (404 for accept and reject, nothing visible to the other user), `test_invalid_decision_bodies_are_rejected` (4 bodies × 2 actions).

Expected Result: accepting a suggestion is exactly an owner edit, rejecting is never asked again, and no owner sees or decides another's suggestions.

## 4. Web

- [x] 4.1 "Saran perbaikan riwayat" section on `/review`: period, estimate and confirmation, progress, last run, groups with before → after, accept and reject per group and per row (D10).
  - Evidence: `src/components/review/HistorySuggestionsSection.test.tsx` (6 tests: axe on the list, the estimate and the empty state; keyboard estimate → confirm payloads; progress from real counts; cancel returns focus; invalid window without request; keyboard group accept; row reject; skipped and failed decisions); `src/lib/historySuggestions.test.ts` (4); `src/app/review/page.test.tsx` passes with the section mounted.
- [x] 4.2 Browser check against seeded synthetic history at 390 and 1280 px, light and dark: keyboard through estimate, confirm, accept and reject; axe on the section; overflow; console errors.
  - Evidence: `verification.md` (browser section).

Expected Result: the owner fixes history from one section by mouse or keyboard, with the cost known before any model call.

## 5. Verification

- [x] 5.1 Backend suite on a freshly recreated PostgreSQL migrated V1→V31; frontend tests, lint, type-check and build; `openspec validate` (change, `--all`, `--strict`).
  - Evidence: `verification.md` with counts and exit codes.

Expected Result: all suites green on a fresh database and the specs validate.

## Workflow follow-up

- Archive with `openspec archive add-history-suggestions --yes`, re-run `openspec validate --all`.
- Commit, push `feat/history-suggestions` and report the compare link `main...feat/history-suggestions`; the owner merges.
