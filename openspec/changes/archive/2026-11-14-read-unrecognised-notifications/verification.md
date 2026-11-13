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
| Backend, full suite (Postgres 16, Flyway V1→V56) | 2321 passed |
| `tests/test_notification_model_reading.py` | 24 passed (production sequence included) |
| Frontend `npm test` (vitest, whole suite) | 1109 passed |
| Frontend `type-check`, `lint` | passed |
| Companion `testDebugUnitTest` | 596 tests, 0 failures, 6 skipped (existing) |
| Companion `lintDebug`, `assembleDebug`, `compileDebugAndroidTestKotlin` | lint 0 errors, 49 warnings (warnings do not fail CI); build and instrumented-test compile pass |
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
- **Owner names an account at another bank:**
  - `test_the_source_question_offers_every_owned_account`: options are pockets, BCA and GoPay; not the target, the pocket-only Jago parent, or the investment account.
  - `test_money_from_another_bank_completes_on_it_and_its_notification_attaches_later`
  - `test_money_from_another_bank_links_the_leg_already_recorded`
  - `test_several_possible_legs_on_the_other_bank_wait_in_review`
- **Production sequence:** `test_the_production_sequence_resolves_once_each`. Result: one mapping question ("Spaylater") followed by a confirmation, three more confirmations, one source question, and four movements in total.
- **Benchmark scoring:** `tests/test_notification_benchmark.py` scores the six new `unread-*` cases and `unsettled` offline. The unnamed-source move is scored as `needs_confirmation`.
- **Companion:** `pocketQuestionPreselectsTheSuggestionButWaitsForTheOwner`, in light and dark themes.

## Paid benchmark (task 3.2, owner OK 2026-11-13)

**Run:**
- Revision `83d359a`, clean working tree. Prompt `notification-interpretation-13`, fixture `synthetic-notifications-6`.
- Partition `all`, `gpt-6-luna` at effort `none`.
- 43 model cases × 3 repeats = 129 requests.
- Cost: upper bound USD 0.729 (cap 0.80); observed USD 0.114, token-priced, not account billing.
- Report: `cash-flow-tracker-prototypes/research/evidence/read-unrecognised-notifications/bench-id-all.json`.

| Prompt | Requests | Schema | Record recall | Unambiguous classification | Movement P / R | Safety | Quality |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `-13`, all (this run) | 129 | 99.2% | 100% | 83.3% | 100% / 69.7% | pass | fail |
| `-12`, development (2026-11-09) | 57 | 98.2% | 98.0% | 100% | 100% / 100% | pass | pass |
| `-12`, held-out (2026-11-09) | 51 | 100% | 100% | 66.7% | 100% / 33.3% | pass | fail |

- **Regressions:** none against `-12`.
- **Quality gate:** it fails for the reason recorded on 2026-11-09. `gopay-bank-out` (self-transfer category) fails every repeat, as it did before this change. That field is decided by the backend, not the model.
- **Other failures:** one `json_decode` failure (`archived-mapping`), and no unsafe validated write.

**New cases** (3 repeats each):

| Case | Result |
| --- | --- |
| `unread-jago-added-locked` | 3/3 `needs_confirmation`. Target quoted as "Bayar Kredit"; the suggested source Jajan matches the recent movements. |
| `unread-jago-topped-up-named` | 3/3 recorded Jajan → Dana Darurat. |
| `unread-bill-reminder`, `unread-locked-unlock-date`, `unread-future-interest` | 3/3 ignored each. |
| `unsettled` ("Transfer Rp50.000 ke BCA") | 3/3 `needs_review`. |
| `unread-jago-income` ("Rp150.000 sudah masuk ke Jajan Pocket kamu.", expected income) | 3/3 read as a move into Jajan with no named source (confidence 0.94). Nothing is recorded: the source question is asked, and the model's suggestion (Jajan, the target itself) is dropped. This question has no "not a pocket move" answer, so a misread income waits in the question; see the open point below. |

## Owner decision on sources outside Jago (2026-11-13)

**Decision:** the source question lists all of the owner's own accounts, not a fixed "outside Jago" answer. Money from another person is not given its own answer; the owner asked not to shape the app around their own cases.

**Changes after the benchmark:**
- source options, recording from another institution, and the companion wording;
- prompt, reading acceptance and benchmark cases are unchanged, so the benchmark still applies.

## Not yet verified

- **Production behaviour after deploy (task 5.3).**

## Acceptance

The owner accepted the result on 2026-11-14 ("ok"). Task 5.3 (companion release and reprocessing the five events) follows the merge and deploy.
