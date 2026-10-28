# Verification: add-resilient-movement-pairing

Date: 2026-10-28. Branch `feat/resilient-movement-pairing`, based on efcf3ef (phase 3, now an
ancestor of `main`). Results below are for the working tree that became the feature commit.

All notification texts, names and amounts in tests are synthetic.

## Database

- Disposable PostgreSQL 16 (`cft-p5-db`, network `cft-p5-net`, `127.0.0.1:55505`), recreated empty,
  then `flyway/flyway:10 migrate`: exit 0, "Successfully applied 30 migrations … now at version v30".
- `test_v30_matches_mirror_and_is_idempotent`: V30 and the startup mirror are identical, the mirror
  applies twice, both columns exist.

## Backend

Command (repository root):

```
PYTHONPATH=backend TEST_DATABASE_URL=postgresql://ledger:ledgerpass@localhost:55505/ledger_test \
  SKIP_TEST_MIGRATIONS=1 <owner venv>/bin/pytest backend/tests -q -p no:cacheprovider -rs
```

- Full suite on the fresh V1→V30 database: **842 passed, 0 skipped, 0 failed** (exit 0). The same
  base before this change: 801 passed.
- New `backend/tests/test_movement_pairing.py`: 41 tests (fee table and each fee, maximum, settings,
  evidence strength, fee pairing with equal legs and fee row, unknown difference, weak evidence with
  a fee, split returns the fee and is never re-paired, delete removes the fee, fee row merge 409, late
  leg 40 minutes apart, purpose question closed by the pass, weak pair outside the window, manual
  "Internal Movement" candidate, owner-reclassified row excluded, concurrent passes, lookback,
  awaiting counterpart not counted as spending, completion of outgoing and incoming legs with
  idempotent replay and every validation negative, repeated-amount ambiguity, strong pair despite a
  repeated amount, reports and rows unchanged by the pass).
- Fail-before check: with the `backend/app` changes stashed, 37 of the 41 new tests fail; the 4 that
  pass are negative cases the old code already met (no pairing).
- Real corpus and pairing set (`test_real_notification_corpus.py`, `test_notification_pairing.py`,
  `test_notification_benchmark.py`, `test_movement_pairing.py`): 145 passed.
- Concurrency and late-pairing tests repeated five times: all passed each time.

## Configuration

- `docker compose -f docker-compose.yml config` (placeholder secrets) renders the five
  `MOVEMENT_*` variables; the deploy workflow parses as YAML.

## OpenSpec

- `openspec validate add-resilient-movement-pairing` and `--strict`: valid.
- `openspec validate --all`: 50 passed, 0 failed.
- `openspec validate --all --strict`: 45 passed, 5 failed, all pre-existing "requirement text is very
  long" warnings in untouched main specs (`account-reconciliation`, `clean-core-ledger`,
  `kakeibo-decoupling`, `kakeibo-reconciliation`, `notification-filtering`). Follow-up, not reshaped
  here.
- `git diff --check`: clean.
- No other open change modifies `notification-record-and-flag`, `transaction-provenance` or
  `movement-pairing`.

## Not verified

- Frontend: no frontend file changed; `npm test`, lint, type-check and build were not run.
- Browser checks: no UI in this phase.
- Production: no deployment, no production data, no companion device run. The 14 historical one-sided
  rows were not inspected or changed.
- The lifespan loop itself was not observed running for ten minutes; the pass it calls is covered by
  the tests above, and the loop follows the existing recurring-scheduler pattern.
