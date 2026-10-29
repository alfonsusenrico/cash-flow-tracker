# Verification: add-ai-text-capture

Date: 2026-10-29. Base is origin/main 2e3135c, plus the working tree of `feat/ai-text-capture`, which was then committed. All data is synthetic. Provider calls are mocked; no model was called.

| Check | Result |
|---|---|
| Fresh `postgres:16`, Flyway V1→V33 | 33 migrations applied |
| Full backend suite | 986 passed, 0 skipped (910 before; 76 new: 46 in `test_capture_parsing.py`, 30 in `test_text_capture.py`) |
| Constraint test on a database migrated only to V32 | fails with `CheckViolation` on `chk_transactions_source` (passes on V33) |
| Key tests against mutated code (scratch copy) | each fails when its behaviour is removed: memory hit skipping the model, undo window, undo-after-edit check, text kept out of logs, movement word table, resend idempotency, stale question check, account answer teaching memory, bare decimals not read as amounts |
| `openspec validate --all --strict` | exit 0 |
| `git diff --check` | clean |
| Frontend | not run: the frontend is unchanged by this change |

Evidence by task:

| Task | Tests |
|---|---|
| 1.1 | `test_database_accepts_capture_source_and_feedback_kind` (capture source and feedback kind accepted, `bot` rejected, a `needs_detail` capture without a question rejected) |
| 1.2 | `test_capture_parsing.py`: every spec amount, the negatives (no amount, two amounts, "1.5", "1,5", "10,000", zero, above 1e12), kinds, "kemarin", "tadi malam", "tadi pagi", "tgl N", day/month, future and impossible dates, account mentions, aliases and movement roles |
| 2.1 | `test_known_merchant_from_memory_records_without_model` (mock asserts no call), `test_confident_model_records_new_merchant_as_ai`, `test_capture_normalizer_uses_its_own_strict_schema_and_prompt`, `test_normalization_validation`, `test_provider_unavailable_asks_category_with_ranked_chips` (provider error, invalid reference, AI disabled) |
| 2.2 | `test_unknown_account_is_asked_most_recent_first`, `test_clear_history_resolves_the_account`, `test_amount_questions`, `test_kind_question_from_words_or_model_flag`, `test_low_confidence_or_changed_threshold_asks_category`, `test_answers_are_idempotent_and_stale_questions_conflict`, `test_account_answer_validation` |
| 2.3 | `test_cash_withdrawal_is_a_movement_from_the_usual_account` (equal legs, not reportable as spending or income, balances), `test_movement_without_history_asks_source_and_named_movement_records`, `test_answers_teach_memory_and_the_second_capture_needs_no_question`, `test_account_answer_with_model_category_teaches_memory`, `test_text_naming_the_merchant_teaches_owner_memory`, `test_yesterday_is_parsed_in_jakarta` |
| 2.4 | `test_undo_within_window_removes_rows_and_restores_balance_and_memory` (4 s), `test_undo_too_late_or_after_an_edit_changes_nothing` (20 s, edited row, unrecorded capture), `test_undo_of_a_movement_removes_both_legs`, `test_undo_of_a_learned_capture_removes_the_new_memory` |
| 2.5 | `test_invalid_and_foreign_requests_change_nothing` (422 and 404), `test_resend_returns_the_same_capture`, `test_capture_rate_limit`, `test_logs_hold_no_text_or_amounts`, `test_capture_threshold_setting` |

**Not verified:**
- Classification quality of a real model with the `capture-normalization-1` prompt. No evaluation set exists for typed captures, and the phase forbids real model calls. Follow-up: a synthetic capture eval before raising reliance on the model.
- The companion UI. It follows in the mobile change.
- The web ledger shows the new source as the raw value `capture` until `frontend/src/lib/provenance.ts` gains a label. The frontend was out of scope for this change.
- Production. Deployment is through the pipeline after the owner merges.
