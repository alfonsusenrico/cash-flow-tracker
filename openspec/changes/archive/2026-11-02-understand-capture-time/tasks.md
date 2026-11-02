# Tasks

## 1. Deterministic time reading (D1–D3)

- [x] 1.1 Time reader `services/capture_time.py`: dates, weekdays, vague periods, days ago, relative days, offsets, clock times (Indonesian, English, number words, half and quarter hours), parts of the day, `tadi`/`barusan`, and the flagged-phrase lexicon. Evidence: `test_capture_time.py::test_resolved_phrases` (69 phrase × send-time cases, the owner's "tadi jam 2" at 15:00 among them).
- [x] 1.2 The D2 resolution rule: readings, day modes, the 15-minute rounding tolerance, deep-night plausibility, 24-hour and 12-hour windows, night after midnight. Evidence: `test_resolved_phrases` (2 AM vs 2 PM at 01:00, 03:00 and 15:00; 23:00 before midnight; DST-free Asia/Jakarta), `test_ambiguous_hours_are_asked` (3), `test_device_time_in_another_offset_is_read_in_the_owner_timezone`, `test_daylight_saving_zone_keeps_wall_clock_readings`.
- [x] 1.3 What is asked and what stays 422. Evidence: `test_last_week_is_asked_as_a_day`, `test_impossible_or_conflicting_times_are_asked` (5), `test_unusable_explicit_dates_are_still_rejected` (5), and the unchanged `test_capture_parsing.py` (46).
- [x] 1.4 Non-time words keep the device time, the amount and the description; time words leave the description and the memory key. Evidence: `test_words_that_are_not_times_keep_the_device_time` (8), `test_non_time_words_keep_amount_and_description` (3), `test_meal_words_stay_in_the_description_and_time_words_do_not`, `test_stated_date_with_time_keeps_the_amount`.

Expected Result: the owner's phrases resolve the way a person reads them, and nothing resolves to the future.

## 2. Model fallback, question and storage (D4–D9)

- [x] 2.1 Strict `time` output, prompt `capture-normalization-2`, model context `sent_at`/`time_phrase`/`time_day`, and the model asked for the time alone when memory classifies. Evidence: `test_model_reads_a_flagged_phrase_even_when_memory_classifies`, updated `test_capture_normalizer_uses_its_own_strict_schema_and_prompt`.
- [x] 2.2 Fallback checks with outcome-only logging. Evidence: `test_model_time_is_accepted_only_for_a_flagged_phrase`, `test_model_time_rejections` (8), `test_model_time_below_the_threshold_is_only_a_suggestion`, `test_model_time_must_fall_on_the_parsed_day_and_is_ignored_when_not_needed`, `test_rejected_model_time_asks` (4, mocked provider), `test_unavailable_model_asks_the_time`, `test_model_never_changes_the_amount`.
- [x] 2.3 The `time` question after the amount, chips, typed answers and their validation. Evidence: `test_ambiguous_hour_is_asked_and_answered_with_a_chip` (422 for an account id and an unknown chip), `test_typed_answer_is_read_with_the_open_phrase` (422 for "entahlah" and "besok jam 8"), `test_time_is_asked_after_the_amount`, `test_last_week_asks_the_day`, `test_answers_are_read_with_the_open_phrase_first`, `test_labels_name_the_day_and_clock`.
- [x] 2.4 `question_types` and the older-phone behaviour. Evidence: `test_older_phone_without_time_questions_keeps_todays_behaviour`, `test_invalid_question_types_are_rejected`; read-only check of the phone's `CaptureModels.kt` and `CaptureRules.isAnswerable` (design D7, D9).
- [x] 2.5 Migration V40, basis and evidence on capture rows and both movement legs, the owner date edit, unchanged notification times. Evidence: Flyway V1→V40 on a fresh PostgreSQL; `test_owner_example_records_two_in_the_afternoon`, `test_text_without_time_words_keeps_the_device_time`, `test_both_legs_of_a_typed_movement_carry_the_time`, `test_owner_date_edit_makes_the_time_the_owners`, `test_notification_rows_keep_their_post_time`, `test_database_rejects_unknown_time_basis_and_stray_evidence`.
- [x] 2.6 Suggestions strip every time word from a reused prefill. Evidence: `test_time_word_spans_cover_every_phrase_kind`, unchanged `test_capture_suggestions.py`.

Expected Result: an undecidable time is asked or read by a checked model, and every capture row says how its time was decided.

## 3. Verification and delivery

- [x] 3.1 Backend suite on a fresh PostgreSQL 16 migrated V1→V40, and a CI-style run without a database. Evidence: counts in verification.md.
- [x] 3.2 `git diff --check`, `openspec validate --all --strict`, and a scenario-name check of the MODIFIED requirements. Evidence: verification.md.
- [x] 3.3 Archive the change, commit, push `feat/capture-time-understanding` and report the compare link; the owner opens and merges the pull request. Evidence: `git log` and the push output.

Expected Result: verified, archived and pushed for owner acceptance.
