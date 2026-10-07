# Framework adoption verification — 2026-10-07

## Tested revision and scope

Branch `chore/adopt-agent-framework` from main `4db742f94c601112c99f0358cdecd97da25afca7`. Application source, tests, CI, deployment scripts, tokens and root design.md are unchanged. Local runtime: Python 3.14.7, Node 26.9.0, PostgreSQL 16 (`postgres:16-alpine`), OpenSpec 1.13.1. CI runtimes were not rerun for this documentation branch.

The documentation diff consists only of AGENTS.md, openspec/config.yaml, existing spec summaries/formatting, research documentation and empty research scaffold markers. The spec repair replaces 27 Purpose placeholders and reformats 108 long requirement bodies in 53 files. Requirement names, every original contract clause (ignoring indentation) and all pre-existing scenarios are preserved. No real financial data, notification samples or screenshots were added to research.

## Framework and instruction checks

```sh
af project sync . --replace-legacy --apply
af project check .
```

Executed via the installed framework repository's tools/af (not installed on shell PATH). No profile argument; no global installation or hook installation. Before check: **2 errors / 11 warnings**. After: **0 errors / 0 warnings**. Pinned digest version 0.1.0, SHA-256 `a62e9a3fd55a6a9a160ff5ff9e8d9947c1bceba56e134133c06359a12b0a7141`. All relative AGENTS links resolve.

AGENTS.md: **18,602 → 8,760 bytes**, project content **6,775 characters**. Local PROJECT_STATE.md: **100,939 → 5,085 bytes** at compaction; delivery receipts remain below 8 KB. Original state is byte-identical to its external private backup. Original checkout's 11-entry dirty status is unchanged. Local state/journal are ignored; private journal/backup mode 0600 and backup directory 0700.

## Native OpenSpec checks

```sh
openspec context --json
openspec validate --all --strict
```

Context resolves this repository's native root and accepts the real project configuration. A fresh isolated copy of main's tracked OpenSpec files gives the strict baseline: **7 passed / 37 failed**, exit 1. Failures include Purpose placeholders and requirements over 500 characters. The earlier 17/27 baseline and 44/0 summary-only result were non-strict; final verification corrects that mismatch.

After summary and format repair: **44 passed / 0 failed**, exit 0, using exactly `openspec validate --all --strict`. Detailed clauses move into explicit WHEN/THEN scenarios under concise normative introductions. Existing requirement names, original contract clauses and prior scenario bodies were mechanically compared against main and retained. MODIFIED deltas retain the new scenario names for archive compatibility. Existing contracts, including obsolete presets, remain for separately approved reconciliation. Checks were not weakened; no change was archived or generated integration modified. Local logs: /private/tmp/cft-adoption-openspec-baseline-strict.log and /private/tmp/cft-adoption-openspec-final.log.

## Backend regression suite

```sh
TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5548/ledger_test SKIP_TEST_MIGRATIONS=1 PYTHONPATH=backend /Users/enrico/enrico/project/enrico/cash-flow-tracker/backend/.venv/bin/python -m pytest backend/tests -q --tb=short
```

**716 passed, zero skipped** (19.78 s), exit 0. All 26 versioned migrations were applied in numeric order to a verified empty, separately named disposable database. The existing host migration fixture lacks Flyway/psql, so SKIP_TEST_MIGRATIONS bypassed only that fixture after independent migration application; it did not skip database tests. Synthetic data only; existing local stacks were not used as test data. Local output reference: /private/tmp/cft-adoption-backend-tests.log.

## Frontend checks

Run in frontend/: `npm ci --no-audit --no-fund`, `npm test`, `npm run type-check`, `npm run lint`, `npm run build`.

All passed; **148 tests in 34 files** (8.16 s). Build compiled and generated all nine static pages. Locked installation and build used authorized network access; no package/source changes resulted. Existing npm install-script restrictions remained in effect; no package scripts or hooks were enabled to make checks pass. Local output reference: /private/tmp/cft-adoption-frontend-tests.log.

No separate backend formatter/type-checker or Prettier command exists in this project's current CI; adoption does not claim to add them. No browser or assistive-technology acceptance was run because the UI did not change; token/accessibility gaps are recorded follow-ups.

## Hook and privacy checks

Inspected executable shared pre-commit, post-commit, post-checkout and post-merge Bob hooks. Existing refs/notes/bob has five top-level tree entries and remains unchanged. Pending notes in the original checkout are empty; the adoption worktree has no pending-note file. No tracked code/CI dependency was found. No note contents were inspected or published.

core.hooksPath stays unset. Framework hooks are intentionally **not installed**, awaiting the owner's coexistence decision; af project check does not diagnose that state in this installed version. No hook was bypassed. PROJECT_STATE.md/JOURNAL.md exclusions were added/preserved in local shared Git metadata, not committed. The original large state was not rewritten because the owner prohibited touching that checkout.

## Review and delivery

`git diff --check` passed. Scope checks confirm no differences under backend/, frontend/, android/, telegram-bot/, scripts/, .github/, docker-compose.yml or docker-compose.local.yml. Root design.md is byte-identical to main. A read-only comparison against base 4db742f checks all changed specs: **199 original requirement bodies** remain verbatim after whitespace normalization, requirement names match exactly, and **489 pre-existing scenarios** remain byte-for-byte intact. No application file changed after the test runs. Section map, capability justification, language sources and follow-up proposals are in ../notes/2026-10-07-agents-md-adoption.md.

Prerequisite commits and final branch receipts are recorded in local PROJECT_STATE.md and the delivery reply. The owner creates/merges the review request. No deployment, production access, settings/secret changes, merge, or hook installation occurred.
