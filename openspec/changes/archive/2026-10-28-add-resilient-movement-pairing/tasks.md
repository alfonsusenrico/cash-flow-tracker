## 1. Schema and settings

- [x] 1.1 Add `db/migrations/V30__movement_pairing_fees_and_splits.sql` and its identical mirror `backend/app/db/movement_pairing_fees_and_splits.sql`, run last by `init_db.py` (design D12).
  - Evidence: `test_v30_matches_mirror_and_is_idempotent` (files identical, mirror applied twice, both columns present); Flyway migrates a fresh database V1→V30.
- [x] 1.2 Add the five pairing settings with validation and the `movement_pairing_configuration_invalid` code; wire them through `.env.example`, `docker-compose.yml`, `scripts/materialize_runtime_env.py` and the deploy workflow (D10).
  - Evidence: `test_default_pairing_settings`, `test_pairing_settings_validation` (valid custom table, fee above maximum, negative maximum, short interval, non-numeric gap); `docker compose config` shows the variables.

Expected Result: the schema stores split markers and fee links, and pairing is configurable with explicit errors.

## 2. Pairing rules

- [x] 2.1 `pairing_evidence` with strong and weak links and `remote_root_account_id` in transfer signatures; `signatures_compatible` keeps its meaning (D4).
  - Evidence: `test_evidence_strength`; the existing pairing unit tests in `test_notification_pairing.py` pass unchanged.
- [x] 2.2 `services/movement_pairing.py`: candidates incl. manual "Internal Movement" rows and excluding owner-split and owner-reclassified rows, fee rule, two-way uniqueness, weak-pair uniqueness, ambiguity flags (D1–D4).
  - Evidence: `test_transfer_fee_table`, `test_fee_above_the_maximum_difference_is_never_tolerated`, `test_manual_internal_movement_row_is_a_candidate`, `test_owner_reclassified_notification_row_is_not_a_candidate`, `test_weak_pair_with_a_repeated_amount_is_flagged_not_linked`, `test_strong_pair_is_linked_despite_a_repeated_amount`; existing ambiguity and window tests in `test_notification_processing_database.py` and `test_ingestion_memory_and_purpose.py` pass.
- [x] 2.3 `record_observed_leg` uses the shared decision, links with a fee, flags competing legs, and adds `awaiting_counterpart` to unpaired "Internal Movement" legs (D1, D5, D9).
  - Evidence: `test_known_fee_pairs_with_equal_legs_and_a_fee_row` for each fee (equal legs, fee row fields, source balance, notification result, ledger `fee_for_movement_id`), `test_unknown_or_too_large_difference_is_not_paired`, `test_weak_evidence_never_tolerates_a_fee`, `test_one_sided_self_transfer_awaits_its_counterpart_and_is_not_spending`.

Expected Result: fee transfers become one equal-legged movement plus a fee expense, and doubtful pairs are flagged instead of linked.

## 3. Undo, split and re-pairing

- [x] 3.1 Split returns the fee and marks both legs `owner_split_at`; delete removes the fee row; merge rejects fee rows (D5–D7).
  - Evidence: `test_split_returns_the_fee_and_is_never_re_paired`, `test_deleting_a_movement_deletes_its_fee`, `test_fee_row_cannot_be_merged_into_a_movement`; existing split tests pass.
- [x] 3.2 Deferred pass and lifespan loop with count-only logs (D8).
  - Evidence: `test_late_leg_is_paired_by_the_re_pairing_pass` (40 minutes apart, shared record key, both `auto`, second run pairs nothing), `test_late_pair_closes_the_purpose_question`, `test_weak_pair_is_not_linked_outside_the_immediate_window`, `test_concurrent_passes_pair_once`, `test_pass_ignores_legs_recorded_before_the_lookback`, `test_awaiting_leg_is_paired_when_the_counterpart_arrives_late`, `test_reports_are_unchanged_for_existing_movements`.

Expected Result: late and hand-entered legs are linked within one pass interval, never twice, and never after an owner split.

## 4. Completing a one-sided transfer

- [x] 4.1 `POST /api/movements/complete` (D9).
  - Evidence: `test_complete_outbound_leg_creates_the_inbound_leg_once` (created leg, reviewed legs, result, idempotent replay, 409 `already_linked`), `test_complete_inbound_leg_creates_the_outbound_leg` (incl. 422 `counterpart_side_mismatch`), `test_complete_validation` (no or both accounts, unknown and foreign transaction, foreign account, same account, investment position, parent with pockets, ordinary category; nothing linked).

Expected Result: the owner can finish any awaiting transfer through the API; phase 6 adds the UI.

## 5. Verification and delivery

- [x] 5.1 Run the full backend suite on a freshly recreated disposable PostgreSQL migrated V1→V30 with `SKIP_TEST_MIGRATIONS=1`, including the real-corpus, benchmark and pairing tests.
  - Evidence: pass and skip counts in `verification.md`.
- [x] 5.2 Run `openspec validate add-resilient-movement-pairing`, `openspec validate --all`, both with `--strict`, and `git diff --check`.
  - Evidence: zero errors; strict warnings recorded as follow-ups.
- [x] 5.3 Write `verification.md` with revision, commands, counts, synthetic-data note and what was not verified.
  - Evidence: the file is complete.
- [x] 5.4 Check open changes for conflicting deltas, archive, re-run `openspec validate --all`, commit, push `feat/resilient-movement-pairing` and report the compare link.
  - Evidence: archive exit status 0, validation output, pushed branch.
