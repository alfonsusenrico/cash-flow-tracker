# Verification: learn-payment-pockets

Tested on 2026-11-14, branch `feat/learn-payment-pockets` on base `f1df169` (main after PR #115).

## Evidence for the rule (production, read-only, 2026-11-14)

The owner's 10 Jago payments that named no pocket, 28 Sep to 13 Nov:

- 8 of the 9 recorded on the default pocket were later moved by hand.
- Grouped by merchant first word they are consistent: `spbu` 3/3 Transportasi, `kredivo` 2/2 Cicilan, debit card 2/2 Subscription.
- Grouped by category they are not: "Tagihan & Utilitas" went to 3 pockets, "Makanan & Minuman" to 4.
- `test_replay_of_the_owners_ten_payments` replays them through the rule: 1 recorded automatically and correctly (the 13 Nov fuel payment), 0 wrong, 9 asked, 3 of those with the right pocket preselected.

## Automated checks

| Check | Result |
| --- | --- |
| Backend, full suite (Postgres 16, Flyway V1→V56) | 2352 passed; the 1 failure, a fixture without a pocket, was fixed and its file rerun (18 passed) |
| `tests/test_payment_pocket_learning.py` | 29 passed |
| Companion `testDebugUnitTest` (in `mobile/`) | 648 tests, 0 failures, 6 skipped (existing) |
| Companion `lintDebug`, `assembleDebug` | lint 0 errors, 51 warnings (warnings do not fail CI); build passes |
| `openspec validate --all --strict` | 85 passed, 0 failed |

## Scenario evidence

All scenarios are tested on synthetic data in `backend/tests/test_payment_pocket_learning.py`.

**No assumed pocket**
- First payment asks with the default preselected: `test_first_payment_to_a_merchant_asks_with_the_default_preselected`.
- Incoming money keeps the default: `test_incoming_money_keeps_the_default_pocket`.
- Older companion: `test_an_older_companion_reviews_without_assuming`.
- Named pocket: `test_a_payment_naming_its_pocket_is_not_asked`.

**Choices**
- A web correction teaches and replaces the answer: `test_a_web_correction_teaches_and_replaces_the_answer`.
- Manual Tinjau resolution keeps the owner's account: `test_manual_resolution_validates_references_and_state`.

**Learned recording**
- Two agreeing choices record, with the shown basis: `test_two_agreeing_answers_record_the_next_payment_and_say_why`.
- One answer only preselects: `test_one_answer_only_preselects`.
- Thin evidence with an exception keeps asking: `test_thin_evidence_with_an_exception_keeps_asking`.
- Rule table: `test_rule_of_succession` (including 5 of 6), `test_two_corrections_away_from_a_habit_ask_again` and `test_only_the_latest_ten_choices_count`.
- Archived pocket: `test_an_archived_learned_pocket_is_asked`.
- Kill switch: `test_kill_switch_asks_with_the_learned_pocket_preselected` and `test_kill_switch_never_records`.

**Key**
- `test_one_station_chain_shares_a_key`, including the production spellings with zero-width spaces.
- `test_debit_card_payments_share_the_notification_kind`.
- `test_other_merchants_are_learned_separately`.

**Moves into a pocket**
- `test_money_into_a_pocket_learns_its_usual_source_and_mixed_sources_keep_asking`: mixed sources ask with the favourite preselected, and 5 of 6 agreeing records with its basis.

**Companion**
- `HabitLabelUiTest.detailSaysThePocketCameFromEarlierChoices`, in light and dark themes at font scales 1 and 2.

## Changed test fixtures

The shared sample payment "You've paid Rp125.000 to Kedai Awan" named no pocket and was recorded on Main Pocket by assumption. Tests that use it for other behaviour now name the pocket ("… using your Kantong Utama Pocket."). Payments without a pocket are covered by the tests above.

## Not yet verified

- Production behaviour after deploy, including the owner's earlier corrections counting as choices.

## Acceptance

The owner accepted the result on 2026-11-14 ("accept"). The production check after deploy follows the merge.
