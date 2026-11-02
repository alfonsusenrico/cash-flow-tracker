# Tasks

## 1. Tinjauan shows only what needs the owner (D1, D2)

- [x] 1.1 Exclude linked legs from the queue, the summary and the monthly report counts with one shared count query; drop the movement group kind and `editable`. Evidence: `test_linked_legs_never_reach_the_queue_or_counts` (queue, summary and the monthly report's `review_counts`; stored states unchanged), updated `test_queue_lists_every_section_and_the_badge_counts`.
- [x] 1.2 `counts.attention` and `counts.unreviewed` in the queue equal the summary. Evidence: `test_queue_counts_split_attention_from_optional_rows`.
- [x] 1.3 Tinjauan sections "Perlu jawaban" and collapsed optional "Dicatat otomatis", header counts. Evidence: `app/review/page.test.tsx` "shows every section…" (headings, header numbers, closed `details`, axe); browser check (keyboard opens the section).

Expected Result: completed movements never appear or count, and automatic rows are clearly optional.

## 2. Settle a debit as a top-up (D3)

- [x] 2.1 Awaiting card "Ini top up investasi" with "Induk · Produk" products and the unit-tracked explanation. Evidence: `app/review/page.test.tsx` "settles a debit as a top-up and explains unit-tracked funds" (request body, no request without a product, link to Rekening, axe) and "does not offer a top-up for an incoming leg".
- [x] 2.2 Ledger dialog block under the type toggle. Evidence: browser check at 390 × 844 px (block inside the viewport without scrolling).
- [x] 2.3 `recurring_rule_id` in `GET /api/transactions`. Evidence: `test_ledger_lists_the_recurring_rule_of_a_row`, existing `investmentTopups.test.ts` recurring case.
- [x] 2.4 Conversion from Tinjauan through the API. Evidence: `test_tinjau_conversion_settles_an_awaiting_debit`.

Expected Result: an RDN debit can be made a top-up from Tinjauan and from the first screen of the ledger dialog.

## 3. Learn top-ups (D4–D9)

- [x] 3.1 Migration V43 `investment_topup_feedback`. Evidence: Flyway V1→V43 on a fresh `postgres:16`.
- [x] 3.2 Evidence, per-product agreement and assignment (pure functions and query). Evidence: `test_three_confirmations_qualify_and_two_only_suggest`, `test_a_correction_weighs_twice_against_the_product_left_behind`, `test_a_rejection_outweighs_one_confirmation_and_counts_against_every_product`, `test_nine_of_ten_weight_passes_exactly_at_the_threshold`, `test_only_the_latest_ten_labels_count`, `test_one_to_one_assignment_and_its_limits`, `test_different_notification_text_is_another_pattern`, `test_same_amount_pair_is_assigned_one_per_product`, `test_a_filled_partner_leaves_the_other_product`.
- [x] 3.3 Suggestion in the queue, accept/correct/reject recording, idempotent replays, user scoping, backfill from `converted_from`. Evidence: `test_existing_conversion_is_the_first_confirmation`, `test_older_confirmation_is_worded_as_before`, `test_accept_and_correct_record_one_answer_each` (replays store nothing), `test_rejection_is_remembered_and_weighs_twice`, `test_rejecting_a_foreign_or_linked_transaction_changes_nothing`, `test_suggested_product_of_another_owner_is_refused`, `test_undone_conversion_is_no_longer_evidence`, `test_archived_product_is_never_suggested`, `test_other_users_conversions_are_never_evidence` (mutation-checked: dropping the owner filter fails it).
- [x] 3.4 Automatic recording at ingestion with switch, savepoint and restore. Evidence: `test_third_confirmation_records_the_next_debit_automatically` (money, provenance, basis, no feedback, notification replay records nothing, automatic top-ups are not evidence; mutation-checked), `test_switch_off_only_suggests`, `test_below_the_threshold_only_suggests`, `test_pair_is_recorded_one_per_product`, `test_deleting_an_automatic_topup_returns_the_debit_and_rejects` (mutation-checked), `test_a_refused_conversion_leaves_the_debit_awaiting`.
- [x] 3.5 Web suggestion card ("Ya, top up", "Pilih produk lain", "Bukan top up", choice without default). Evidence: `app/review/page.test.tsx` "accepts, corrects or rejects a learned suggestion" (axe) and "asks without a default when the product cannot be assigned"; browser check.
- [x] 3.6 `TOPUP_AUTO_RECORD_ENABLED` in config, compose and `.env.example`. Evidence: `docker compose config --quiet`.

Expected Result: converted debits are remembered, suggested next time and recorded automatically after three confirmations.

## 4. Verification and delivery

- [x] 4.1 Backend suite on a fresh PostgreSQL 16 migrated V1→V43 with `REQUIRE_TEST_DATABASE=1`. Evidence: counts in verification.md.
- [x] 4.2 Frontend `npm ci`, type-check, lint, test, build. Evidence: counts in verification.md.
- [x] 4.3 Headless Brave over CDP at 390 and 1280 px, light and dark, with synthetic data: Tinjauan sections, top-up option and suggestion, ledger block without scrolling at 390 px, keyboard, console, axe. Evidence: verification.md.
- [x] 4.4 `git diff --check`, `openspec validate --all --strict`, scenario names of MODIFIED requirements kept, final diff review. Evidence: verification.md.
- [x] 4.5 Archive the change, commit, push `feat/review-clarity-and-topup-learning` and report the compare link; the owner opens and merges the pull request. Evidence: `git log` and the push output.

Expected Result: verified, archived and pushed for owner acceptance.
