# Verification: add-capture-suggestions

Date: 2026-10-29. Base is `feat/ai-text-capture` at 69343da, plus the working tree of this change, which was then committed. All data is synthetic. No model is called by this change, and none was called by the tests.

| Check | Result |
|---|---|
| Fresh `postgres:16`, Flyway V1→V33 | 33 migrations applied (no new migration) |
| Full backend suite on that database | 1008 passed, 0 skipped (986 before; 22 new in `test_capture_suggestions.py`) |
| Key tests against mutated code (scratch copy) | each fails when its behaviour is removed: notification rows admitted, movement and fee exclusion removed, undone skip removed, 90-day window widened, hour weight 0, weekday-type weight 0, cap raised to 8, date-word check on reused texts removed |
| `openspec validate --all --strict` | exit 0 |
| `git diff --check` | clean |
| Frontend | not run: the frontend is unchanged by this change |

Evidence by task:

| Task | Tests |
|---|---|
| 1.1 | `test_morning_habit_ranks_first_and_time_of_day_changes_the_order` (the spec's morning scenario with the exact suggestion, the no-history scenario, and the order flipping at 15:30), `test_weekday_type_decides_between_equal_habits`, `test_bank_notified_movement_fee_and_unreviewed_rows_are_not_suggested`, `test_a_group_whose_last_entry_was_undone_is_skipped`, `test_only_the_last_90_days_count`, `test_at_most_six_suggestions_best_first`, `test_prefill_text_is_built_from_shorthand_when_no_typed_text_fits` (label with its own amount, income verb, mode and tie, a capture text with "kemarin", a capture whose amount was answered), `test_amount_shorthand_parses_back_to_the_same_amount` (12 amounts), `test_built_text_keeps_the_kind_and_the_length_limit` |
| 1.2 | `test_invalid_device_time_and_unauthenticated_requests` (missing, naive, unparseable, empty, future and oversized values give 422; no session gives 401 even with a bad value), `test_another_owners_history_is_never_used` |

A request recorded in a scratch copy (five morning parking captures, six afternoon coffee rows, one income row) returned this body to `GET /api/capture/suggestions?device_time=2026-10-28T08%3A30%3A00%2B07%3A00`:

```json
{"ok": true, "suggestions": [
  {"text": "parkir ptc 10rb", "label": "Parkir PTC", "amount": 10000, "category_name": "Parkir", "account_name": "Tunai", "reason": "biasanya jam segini"},
  {"text": "kopi senja 32rb", "label": "Kopi Senja", "amount": 32000, "category_name": "Jajan & Nongkrong", "account_name": "GoPay", "reason": "sering"},
  {"text": "gaji freelance 1,5jt", "label": "Gaji freelance", "amount": 1500000, "category_name": "Gaji", "account_name": "Bank Jago · Kantong Utama", "reason": "terakhir"}
]}
```

A naive `device_time` returned 422 `{"ok": false, "detail": {"code": "invalid_device_time"}}`.

**Not verified:**
- The companion screen that shows the chips. It follows in the mobile change.
- Ranking quality on real history. The scoring weights come from the design and were checked only on synthetic habits.
- Production. Deployment is through the pipeline after the owner merges.
