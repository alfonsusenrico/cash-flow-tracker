## Verification: absorb-one-report-without-link

**Revision tested:** the working tree on `fix/seen-twice-absorb-once`, based on `dd67c58` (main). The tested tree is the one committed.

**Data:** the tests use synthetic data. The field replay used the owner's Sesi uji export, read-only from production with the owner's go-ahead. It was replayed only on a loopback database and deleted afterwards. Only the scores are recorded here.

### Red before, green after

On unchanged `main` both new tests fail:
- `test_a_kept_leg_absorbs_one_report_per_app`: the second Jago report repeats the first GoPay leg;
- `test_a_linked_second_top_up_is_skipped_against_its_own_leg`: the report is held instead of skipped.

Both pass with the fix.

### Suites

| Check | Result |
| --- | --- |
| `test_duplicate_arrivals.py`, `test_find_duplicate_arrivals.py` and `test_balance_check.py` | 60 passed |
| `pytest tests/ -ra` (pinned CI tools, PostgreSQL 16 migrated V1 → V49 by Flyway 10) | 1845 passed, 0 failed, 0 skipped |
| Sesi uji replay (deterministic, loopback) scored with `score_test_session.py` | 18 passed, 0 failed (17 of 18 on `dd67c58`, where step 10a failed) |
| `openspec validate --all --strict` | 81 passed, 0 failed |
| `git diff --check` | clean |

### Not verified

- **Phone:** no new phone run. The next real repeated top-up is the field check.
- **Companion:** the companion's "Di ponsel" sorting of the two Jago → ShopeePay notifications is out of scope and tracked separately.
