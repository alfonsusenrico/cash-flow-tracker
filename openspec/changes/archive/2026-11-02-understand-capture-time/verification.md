# Verification

All runs on 2026-11-02, local only, on the working tree of branch `feat/capture-time-understanding` (base `5004b11`). Synthetic data; the provider is mocked and no model was called.

## Database

- Disposable `postgres:16-alpine` (container `cft-ct-db`, loopback port 55681), database recreated empty, then `flyway/flyway:10 migrate`: "Successfully applied 40 migrations … now at version v40".

## Backend

- With the database (`SKIP_TEST_MIGRATIONS=1`, pytest 9.0.3 as in the owner's environment): **1363 passed, 1 failed**. The failure, `test_plan_auto_link.py::test_one_certain_payment_is_linked_once_and_applies_the_debt_once`, also fails on the untouched base `5004b11` in the same environment (1228 passed, 1 failed before this change) and does not touch captures.
- Without a database (as CI runs): **779 passed, 585 skipped**.
- New tests: 135 (`test_capture_time.py` 116, of which 69 are resolved phrase × send-time cases and 20 more are table rows for ambiguous, conflicting, flagged and non-time phrases; `test_capture_time_flow.py` 19).
- Environment note: with pytest 9.2.0 (the newest, which CI's unpinned install would take), 23 database tests that monkeypatch `app.state.notification_provider` fail on the base as well, because its monkeypatch undo deletes an attribute whose previous value was `None`. CI is unaffected because it runs without a database.

## Repository checks

- `git diff --check` (with new files intent-added): clean.
- `openspec validate understand-capture-time --strict`: valid. `openspec validate --all --strict`: 64 passed, 0 failed.
- MODIFIED requirements keep every scenario name: "Deterministic Money Facts" (Indonesian shorthand amounts; Missing or several amounts are asked; Yesterday) and "Targeted Follow-up Questions" (Unknown account is asked; Provider unavailable), plus one added scenario.

## Phone compatibility (read-only)

- `financial-tracker-mobile-listener` at `35dfbeb`: `CaptureModels.kt` uses Gson data classes, so the added `summary` and `transaction` fields are ignored. `CaptureRules.isAnswerable` accepts only `amount`, `kind`, `account` and `category`, so the phone does not declare `question_types` and is never sent a `time` question (design D7). No Android file was changed.

## Not verified

- Real model behaviour of the time fallback (no live calls were permitted); a capture evaluation set is a follow-up.
- Production, deployment and the phone on a device.
