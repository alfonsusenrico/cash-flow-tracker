# Verification: read-unrecognised-notifications

Tested on 2026-11-13. Server: branch `feat/read-unrecognised-notifications` on base `bcdc425`, working tree as committed. Companion: branch `feat/suggested-source-pocket` on base `35dfbeb`.

## Diagnosis (production, read-only, owner-approved 2026-11-13)

- Five Jago events: "You've added Rp X into Bayar Kredit|Spaylater Locked Pocket." from 5 to 13 Nov. All were `needs_review` / `settlement_unproven`, attempted once, with nothing recorded.
- Four of the five were already entered by hand as moves into Bayar Kredit, 43 minutes to 14 hours earlier: Dana Darurat → 75.260, and Makan-Jalan → 108.001, 214.225 and 130.900. The 47.619 move was not entered.

## Reproduction (task 1.1)

`tests/test_notification_model_reading.py` failed on the base revision for the reported reasons:

- facts stopped at `settlement_unproven` with no `needs_reading`;
- the parser assumed `Kantong Utama` as the source for "added into".

## Automated checks

| Check | Result |
| --- | --- |
| Backend, full suite (Postgres 16, Flyway V1→V56) | 2317 passed |
| `tests/test_notification_model_reading.py` | 20 passed (production sequence included) |
| Frontend `npm test` (vitest, whole suite) | 1109 passed |
| Frontend `type-check`, `lint` | passed |
| Companion `testDebugUnitTest` | 596 tests, 0 failures, 6 skipped (existing) |
| Companion `lintDebug` | 0 errors, 49 warnings (warnings do not fail CI) |
| `git diff --check` | clean |
| `openspec validate --all --strict` | 85 passed, 0 failed |

## Scenario evidence

All scenarios are tested on synthetic data in `backend/tests/test_notification_model_reading.py`:

- **Unrecognised settled wording is read:** `test_unrecognised_settled_wording_waits_for_a_reading`, `test_unrecognised_wording_without_a_direction_cue_is_read_not_dropped`. Noise stays ignored (`test_noise_stays_ignored_without_a_reading`).
- **An unsure reading goes to review:** `test_an_unsure_or_unquoted_reading_goes_to_review`, covering low confidence, an absent activity quote and an absent pocket quote.
- **The model judges unrecognised wording as noise:** `test_the_model_can_judge_unrecognised_wording_as_noise`.
- **Model unavailable:** `test_without_the_model_unrecognised_wording_stays_unproven`.
- **Source answer reuses the reading:** `test_owner_answer_records_one_move_without_another_model_call` (one provider call in total).
- **Retry reads afresh:** `test_retry_discards_the_reading_and_reads_again`.
- **Added into a locked pocket asks for the source:** `test_added_into_locked_pocket_asks_for_the_source_with_a_suggestion`. The suggestion is in the question and the options; the target is left out.
- **Owner answers the source of the move:** `test_owner_answer_records_one_move_without_another_model_call`. Result: one movement from Makan-Jalan to Bayar Kredit at the notification's time.
- **Unanswered or stale answer never debits the main pocket:** `test_answer_naming_the_target_or_nothing_never_debits_the_main_pocket`.
- **Other "into" wording is not a main-pocket move:** `test_into_pocket_rule_needs_a_move_verb`.
- **A hand-entered move is confirmed:** `test_a_hand_entered_move_is_confirmed_not_duplicated`.
- **Several hand-entered moves match:** `test_several_hand_entered_moves_ask_then_follow_the_answer`.
- **Outside 48 hours or already reported:** `test_hand_entered_moves_outside_the_window_or_already_reported_are_not_confirmed`.
- **Production sequence:** `test_the_production_sequence_resolves_once_each`. Result: one mapping question ("Spaylater") followed by a confirmation, three more confirmations, one source question, and four movements in total.
- **Benchmark scoring:** `tests/test_notification_benchmark.py` scores the six new `unread-*` cases and `unsettled` offline. The unnamed-source move is scored as `needs_confirmation`.
- **Companion:** `pocketQuestionPreselectsTheSuggestionButWaitsForTheOwner`, in light and dark themes.

## Not yet verified

- **Paid benchmark run (task 3.2):** waits for the owner's OK. Run: 43 model cases × 3 repeats on `gpt-6-luna`, effort `none`. Conservative estimate ceiling: USD 0.73.
- **Production behaviour after deploy (task 5.3).**
