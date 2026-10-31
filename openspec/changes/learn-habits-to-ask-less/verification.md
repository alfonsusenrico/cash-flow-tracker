# Verification

All data is synthetic. No production host, production database, real notification, phone or real model call was used; the purpose normaliser and the notification provider are mocks in tests.

## Revision

Branch `feat/habit-learning` from `origin/main` `e1bde53`. Checks ran on the final working tree of the feature commit on 2026-10-31.

## Checks

| Check | Result |
| --- | --- |
| Flyway (`flyway/flyway:10`) on a newly created `postgres:16` | V1→V39 applied, 39 migrations |
| Backend `pytest backend/tests` on that database | 1205 passed, 0 failed, 0 skipped. The suite had 1169 tests before this change's new file; 36 are new in `test_habit_learning.py` |
| Backend without PostgreSQL (CI style, database cases skip) | 658 passed, 547 skipped, 0 failed |
| Frontend `npm ci` | completed |
| Frontend `npm test` | 55 files, 286 tests passed (9 new) |
| Frontend `npm run lint` | 0 errors, 1 existing warning (`src/app/review/page.tsx` `_done`, unchanged) |
| Frontend `npm run type-check` | passed |
| Frontend `npm run build` | compiled successfully |
| `git diff --check` including new files | clean |
| `openspec validate learn-habits-to-ask-less --strict` | valid |
| `openspec validate --all` | 63 passed, 0 failed |
| MODIFIED deltas keep every source scenario name (script against the main specs) | no scenario dropped; 5 scenarios added |

## Evidence per decision

- **D1 evidence.**
  - `test_ai_classified_and_unconfirmed_labels_are_never_evidence`: unclassified text answers, AI captures, accepted suggestions and pre-V39 answers without a reviewed event give no guess.
  - `test_owner_capture_answers_and_reviewed_legacy_answers_are_evidence`: owner captures plus a reviewed pre-V39 answer reach 3 of 3.
  - `test_labels_in_movement_categories_never_become_a_habit`.
  - `test_another_users_labels_are_never_used`: another owner's rows with the same account id and signature; the evidence query returns nothing for the owner.
  - `test_a_later_correction_replaces_the_answer_of_its_transaction`.
  - Mutation checks: making every row trusted fails the evidence test; removing the owner filter from the evidence query fails the tenant test.
- **D2/D3 key and band.** `test_amount_band_is_a_symmetric_quarter` (7 edge cases, including Rp 0); `test_amount_outside_the_band_is_asked` (Rp 16.000 against Rp 11.500–12.500 labels is asked with no guess).
- **D4 thresholds.**
  - 3 of 3 records; 2 answers only suggest; 3 of 4 (75%) only suggests, also end to end in `test_three_of_four_agreeing_answers_still_ask` (basis 3 of 4).
  - 9 of 10 records exactly at 90%.
  - Nine answers and one correction give 9/11 and ask; three answers and one correction give 3/5.
  - The pillar must agree as well.
- **D5 window.** `test_only_the_latest_ten_labelled_payments_count`; the drift scenario of the simulation.
- **D6 never automatic.** `test_income_and_transfer_shaped_payments_are_never_recorded_from_a_habit` (masked income, "at Transfer" expense). Labels only: amount, account and date equal the notification's in `test_three_agreeing_answers_record_the_next_payment_without_a_question`.
- **D7 recording.**
  - `test_three_agreeing_answers_record_the_next_payment_without_a_question`:
    - the row has `classified_by = habit`, `reviewed`, no confidence, no question and no review reasons;
    - the event stores `source = habit` and the basis;
    - it is not in the review queue, and the attention count is 0;
    - no feedback is written, and the next payment's basis is still 3 of 3.
  - `test_ai_mode_records_from_the_habit_without_calling_the_model`: the provider is not awaited.
  - `test_reprocessing_a_habit_recorded_event_changes_nothing`: same event, one transaction, no new feedback.
  - `test_pairing_a_habit_row_as_a_movement_drops_the_habit_classifier`.
- **D4 corrections.** `test_a_correction_of_a_habit_record_asks_the_next_payment`: the correction row has `before_classified_by = habit` and `after_classified_by = owner`, and the next payment is asked with basis 3 of 4.
- **D8 guess.** `test_two_answers_only_put_the_guess_first` ("Sepertinya Makanan & Minuman?", notes "makan siang", basis 2 of 2); updated `test_suggestions_rank_by_time_amount_and_signature`.
- **D9 metric.**
  - `test_question_rate_counts_each_recording_outcome`: 8 recorded, 4 asked, 3 answered, 1 memory capture, 2 habit, 0 AI, 1 other; previous and current week boundaries.
  - `test_question_rate_counts_corrections_in_their_week`: 1 correction, 1 on a habit row.
  - `test_question_rate_endpoint_has_an_honest_empty_state`: 8 empty weeks, `has_data: false`, 422 for 0 and 27.
  - Web: `QuestionRate.test.tsx`, 4 tests with axe: weekly figures, a week without rows, empty state, failed load.
- **D10 explanation.**
  - Companion result `habit.label` "Dicatat dari kebiasaan: Makanan & Minuman" and `habit.basis` "3 dari 3 jawaban sebelumnya".
  - `habit_basis` in the ledger list and detail.
  - Web: `ledger/page.test.tsx` (row "Dicatat dari kebiasaan · 3 dari 3", dialog label and basis, axe; no label after an owner edit), `MobileLedgerFeed.test.tsx` (axe), `provenance.test.ts` ("Kebiasaan").
- **D11 kill switch.** `test_kill_switch_never_records`; `test_kill_switch_only_suggests` (asked, guess first, `classified_by = rule`).

## Offline evaluation

`PYTHONPATH=backend python3 scripts/verify/habit_simulation.py` gives a deterministic, synthetic 8-week history (200 seeds). The full tables are in `research/evidence/2026-10-31-habit-simulation.md`, and `test_offline_simulation_asks_less_each_week_with_few_wrong_records` guards them. The current behaviour asks every merchant-less payment (100%).

| Chosen configuration | Weeks 1→8 question rate | Habitual lunch | Wrong automatic records |
| --- | --- | --- | --- |
| Steady | 89 67 70 67 65 65 66 64% | 69 35 39 41 36 39 38 36% | 9% of automatic, 3% of payments |
| Lunch habit changes in week 5 | 89 67 70 67 83 80 54 55% | 69 35 39 41 86 80 6 9% | 9% of automatic, 3% of payments |

Alternatives compared in the same run:
- Correction ×1: 65% asked, 8% wrong (steady).
- No window: 77% asked; the changed habit is never relearned.
- ±50% band: 10–11% wrong.
- log2 buckets: 11–12% wrong.

A minimum support of 5 instead of 3 gave 8–9% wrong (2.1–2.2% of payments) for 4 points more questions, so the approved 3 stays.

## Browser checks

Headless Brave over CDP (port 9621) against `next dev` (127.0.0.1:3621) and uvicorn (127.0.0.1:8621, AI disabled, background jobs off) on the iteration database. The data was seeded only through the local API: one owner, three answers (a ledger correction and two taps on the guess), three habit records, a corrected habit record and open questions over four weeks. The seed script asserted that the habit records happened through the real ingestion endpoint.

- Analisis "Seberapa sering ditanya", at 390 and 1280 px in light and dark:
  - no horizontal overflow;
  - the weeks match the seeded history exactly: current week "Ditanya 2 dari 2 (100%)" with "Koreksi: 2 (1 dari kebiasaan)"; previous weeks 1 of 3 and 2 of 4 with "Kebiasaan 2"; first week 2 of 2 answered; older weeks "Tidak ada transaksi tercatat.";
  - dark mode applied (`dark` class, light text on a dark surface).
- Ledger at 1280 px, light and dark: three rows show "Dicatat dari kebiasaan · 3 dari 3". Focusing a habit row and pressing Enter opened "Ubah Transaksi", which showed "Diklasifikasi oleh Kebiasaan" and "Dicatat dari kebiasaan: Makan Siang Contoh · 3 dari 3 jawaban sebelumnya".
- Ledger feed at 390 px, light and dark: three habit items, no overflow.
- Console errors: 0.

## Android (read-only)

`financial-tracker-mobile-listener` main `d33b39a` was read with `git show` and `git grep` only; nothing was edited, built or installed. Gson ignores the new `habit` result field. `classifiedBy` is read only for captures. Suggestion `kind` is not displayed, and a habit suggestion passes `PurposeQuestions.inspect`. A habit record is `reviewed`, so the phone treats it as having no open question. Nothing breaks. Showing the habit label on the phone is a follow-up.

## Not verified

- Device or instrumented checks: no phone was used (out of scope).
- Production behaviour and real-label performance: no production data was read. With one week of real labels (12 answers over 7 categories) few habits will qualify at first.
- Screen-reader walkthrough with an assistive technology: covered by axe and keyboard checks only.

## Rollout and rollback

Deploy through the approved release pipeline; Flyway applies V39 (additive). No phone update is required. To stop automatic recording without a code change, set `HABIT_AUTO_RECORD_ENABLED=false` and restart the API; the guess still comes first. A code rollback keeps existing habit rows valid (see design, Migration and rollback).
