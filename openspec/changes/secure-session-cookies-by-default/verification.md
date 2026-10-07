# Verification

## Scope and tested state

Owner-approved secure-cookie correction, isolated branch `fix/secure-cookies-by-default` from main `7fcd302c93860f3edfe3a054ddc4e4855f07f13e`. All inputs are synthetic; no production connection, deployment, secret changes, or owner-data inspection.

## Before correction

Only the two new regression files and planning artifacts were added; app, workflow, Compose and materializer matched main.

```sh
PYTHONPATH=backend /Users/enrico/enrico/project/enrico/cash-flow-tracker/backend/.venv/bin/python -m pytest backend/tests/test_session_cookie_security.py backend/tests/test_runtime_cookie_security.py -q --tb=short
```

Result: **24 failed, 13 passed** (2.31 s), pytest exit 1. Failures demonstrate insecure absent/blank/non-true defaults, missing production enforcement and Secure response attribute, blank/insecure seed materialization, insecure workflow/base Compose defaults, and the missing local override. Safe local opt-in and explicit materializer values already pass. Local evidence: `/private/tmp/cft-cookie-before.log`.

## After correction

Tested implementation: the final source changes in this branch relative to `7fcd302`; later delivery edits affect only this record and task/state metadata. Local runtime: Python 3.14.7, Node 26.9.0, PostgreSQL 16 (disposable `postgres:16-alpine`). CI's Python 3.12 / Node 22 environment was not executed locally.

The same focused regression command passed: **37 passed** (1.72 s). It verifies absent/blank/non-false defaults, normalized explicit local false, production/default/prod startup refusal, the actual app's synthetic session response, empty secrets, historical seed handling, explicit override preservation, and Compose/workflow contracts. Session tests inspect cookie attributes only, without outputting cookie values.

### Full backend suite

```sh
TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5548/ledger_test SKIP_TEST_MIGRATIONS=1 PYTHONPATH=backend /Users/enrico/enrico/project/enrico/cash-flow-tracker/backend/.venv/bin/python -m pytest backend/tests -q --tb=short
```

All **26 versioned migrations** were applied in numeric order to an independently verified empty, disposable loopback database before testing. `SKIP_TEST_MIGRATIONS=1` avoids the existing fixture's unavailable host Flyway/psql runner; it does not skip database tests. Final result: **716 passed, zero skipped** (14.77 s), exit 0. Evidence: `/private/tmp/cft-cookie-full-retest.log`.

The initial full run had **715 passed / 1 failed** (20.21 s): the existing `test_concurrent_notification_delivery_creates_one_ledger_effect` received `uncertain_institution_mapping`. That test passed in isolation (0.92 s), and unchanged main passed **679 tests** (12.68 s) against a separately migrated baseline database. The final rerun passed without modifying that test or notification application code. The institution resolver and test are identical to main; account fixtures use random hex name suffixes and the resolver matches institution substrings. The transient ambiguity is retained as a separate test-isolation risk, not presented as fixed here. Evidence: `/private/tmp/cft-cookie-full-tests.log`, `/private/tmp/cft-cookie-baseline-tests.log`.

### Materializer dry run and Compose

```sh
python3 /private/tmp/cft-cookie-config-check.py
```

The temporary check ran `scripts/materialize_runtime_env.py --allow-missing --output <temporary-synthetic-file>` in an isolated environment containing PATH only, so COOKIE_SECURE and all actual secrets were unset. It inspected only the non-secret cookie flag and emitted **COOKIE_SECURE=true**, with output mode **0600**. New materializer regressions reproduce this path and the empty-secret/old-seed path on every test run.

The same check captured Compose JSON privately and emitted only the environment/cookie flags:

- `docker compose --env-file /dev/null -f docker-compose.yml config --format json`: **APP_ENV=production, COOKIE_SECURE=true**.
- `docker compose --env-file /dev/null -f docker-compose.yml -f docker-compose.local.yml config --format json`: **APP_ENV=development, COOKIE_SECURE=false**.

No real .env or runtime file was read. Only a synthetic temporary file was produced and removed.

### Release checks and review

Run from `frontend/`: `npm ci --no-audit --no-fund`, `npm run type-check`, `npm run lint`, `npm run build`, and `npm test` all passed; **148 frontend tests / 34 files**. Registry and Google Fonts access initially failed under the sandbox; the authorized network retries succeeded. No frontend source/lockfile changes resulted.

`openspec validate secure-session-cookies-by-default --strict` and `git diff --check` passed. Base and explicit-local Compose rendering passed. No separate backend formatter/linter/type-check command is configured in the existing CI. Review covered changed source, configuration, new tests and artifacts: no remaining blocking finding, no credentials or personal data added, no API/schema changes, and no local override discoverable implicitly by the release script.

## Delivery boundaries

The owner's dirty `fix/investment-topup-label` checkout remains untouched. Local handoff state is maintained in the isolated worktree. Production/server access, deployment, GitHub secret/settings writes, merge and PR creation were not performed. The disposable test container is removed after verification; existing local stacks are preserved.

## Secret name audit

2026-10-07: `gh secret list --repo alfonsusenrico/cash-flow-tracker --json name --jq '[.[] | select(.name == "COOKIE_SECURE") | .name]'` returned `[]`, exit 0. COOKIE_SECURE is absent from repository secrets. Its old workflow fallback therefore likely issued insecure production cookies. Secret values and actual production headers were not inspected.

## Awaiting owner confirmation

After owner merge and pipeline deployment, confirm the production login response Set-Cookie attributes include Secure. Do not share the cookie value. No production runtime claim is made by this branch.

## Separate follow-ups

- Add PR CI and a PostgreSQL service so database tests cannot silently skip.
- Move direct secret interpolation out of workflow scripts and verify SSH host keys.
- Tag deployment images by commit and prove rollback uses the retained artifact.
- Review public port binding, runtime-backed health checks and production runtime-file transfer to the runner.
