# Tasks

## 1. Evidence and decision (D1–D5)

- [x] 1.1 Migration V39: `habit` classifier, feedback classifier and review-state columns, key and latest-label indexes. Evidence: Flyway V1→V39 on a fresh `postgres:16`; `test_three_agreeing_answers_record_the_next_payment_without_a_question` (the database accepts `classified_by = habit`).
- [x] 1.2 Feedback rows record the classifier before and after and the review status after. Evidence: `test_a_correction_of_a_habit_record_asks_the_next_payment` (before `habit`, after `owner`).
- [x] 1.3 Pure decision rule: support of at least 3, 90% weighted agreement in integers, correction ×2, latest label per transaction, latest 10 per key, time only as a tie-break, ±25% ratio band. Evidence: `test_three_agreeing_answers_record_automatically`, `test_two_answers_only_suggest`, `test_three_of_four_agreeing_only_suggest`, `test_nine_of_ten_answers_pass_exactly_at_the_threshold`, `test_a_correction_weighs_twice_an_answer`, `test_the_pillar_is_part_of_the_agreement`, `test_a_later_correction_replaces_the_answer_of_its_transaction`, `test_only_the_latest_ten_labelled_payments_count`, `test_time_of_day_breaks_ties_and_picks_the_wording`, `test_amount_band_is_a_symmetric_quarter` (7 cases).
- [x] 1.4 Evidence query with trust rules, legacy handling and owner scoping. Evidence: `test_ai_classified_and_unconfirmed_labels_are_never_evidence`, `test_owner_capture_answers_and_reviewed_legacy_answers_are_evidence`, `test_labels_in_movement_categories_never_become_a_habit`, `test_another_users_labels_are_never_used` (mutation-checked: removing the owner filter or the trust rule makes these fail).

Expected Result: a habit decision computed only from the owner's own labels, exactly at the approved thresholds.

## 2. Recording and suggestions (D6–D8, D11)

- [x] 2.1 Habit evaluation in the notification context, eligibility, model skip, recording with habit provenance, no feedback or memory, basis on the event. Evidence: `test_three_agreeing_answers_record_the_next_payment_without_a_question` (labels only; amount, date and account unchanged; not in queue or attention; basis stays 3 of 3), `test_ai_mode_records_from_the_habit_without_calling_the_model`, `test_reprocessing_a_habit_recorded_event_changes_nothing`, `test_amount_outside_the_band_is_asked`, `test_income_and_transfer_shaped_payments_are_never_recorded_from_a_habit`.
- [x] 2.2 Corrections re-ask. Evidence: `test_a_correction_of_a_habit_record_asks_the_next_payment` (basis 3 of 4, row asked, classified `rule`).
- [x] 2.3 "Sepertinya <kategori>?" first, without repeating the history group. Evidence: `test_two_answers_only_put_the_guess_first`, `test_three_of_four_agreeing_answers_still_ask`, updated `test_suggestions_rank_by_time_amount_and_signature`.
- [x] 2.4 Kill switch `HABIT_AUTO_RECORD_ENABLED`, read at evaluation, in compose and `.env.example`. Evidence: `test_kill_switch_never_records`, `test_kill_switch_only_suggests`.
- [x] 2.5 Habit rows leave owner-confirmed model history and capture resolution, and become `system` when paired as a movement. Evidence: `test_pairing_a_habit_row_as_a_movement_drops_the_habit_classifier`; full suite unchanged otherwise.

Expected Result: repeated merchant-less payments are recorded without a question, weaker patterns lead the suggestions, and nothing else changes.

## 3. Explanation and metric (D9, D10)

- [x] 3.1 Companion result `habit`, ledger `habit_basis`. Evidence: result and API assertions in `test_three_agreeing_answers_record_the_next_payment_without_a_question`.
- [x] 3.2 `GET /api/dashboard/question-rate`. Evidence: `test_question_rate_counts_each_recording_outcome`, `test_question_rate_counts_corrections_in_their_week`, `test_question_rate_endpoint_has_an_honest_empty_state` (422 for 0 and 27).
- [x] 3.3 Web: Analisis panel, ledger row mark, dialog explanation, "Kebiasaan" classifier label. Evidence: `QuestionRate.test.tsx` (4, axe), `ledger/page.test.tsx` (2 new, axe), `MobileLedgerFeed.test.tsx` (1 new, axe), `provenance.test.ts`, `insights/page.test.tsx`.
- [x] 3.4 Android read-only compatibility check. Evidence: design D10 (main `d33b39a`); follow-up recorded.

Expected Result: every habit record explains itself and the owner sees the weekly question rate from real counts.

## 4. Evaluation and verification

- [x] 4.1 Deterministic offline simulation and its recorded results. Evidence: `scripts/verify/habit_simulation.py`, `research/evidence/2026-10-31-habit-simulation.md`, `test_offline_simulation_asks_less_each_week_with_few_wrong_records`.
- [x] 4.2 Backend suite on a fresh PostgreSQL 16 migrated V1→V39 and a CI-style run without a database. Evidence: counts in verification.md.
- [x] 4.3 Frontend `npm ci`, test, lint, type-check, build. Evidence: counts in verification.md.
- [x] 4.4 Headless Brave over CDP at 390 and 1280 px, light and dark, keyboard and console, on data seeded through the local API. Evidence: verification.md.
- [x] 4.5 `git diff --check`, `openspec validate --all`, scenario-name check of MODIFIED deltas, final diff review. Evidence: verification.md.
- [x] 4.6 Archive the change, commit, push `feat/habit-learning` and report the compare link; the owner opens and merges the pull request. Evidence: `git log` and the push output.

Expected Result: verified, archived and pushed for owner acceptance.

## Follow-ups (not in this change)

- Android: show `habit.label` and `habit.basis` on the phone row.
- Tinjau: show a guess's basis next to the "Sepertinya …?" chip.
- `PLAN_AUTO_LINK_ENABLED` is documented in `.env.example` but, unlike the new habit switch, not passed through `docker-compose.yml`.
