# Verification — 2026-10-31

Branch `chore/small-follow-ups` from origin/main 26e04af. All checks ran locally on synthetic data. No production host, phone or real AI call was used.

## Baseline (26e04af, clean `git archive` export, separate disposable database)

- Backend: 1207 passed on PostgreSQL 16 migrated V1→V39.
- Frontend (`vitest run`): 56 files, 310 tests passed.
- `openspec validate --all --strict` exit 1: `observed-insights` (2 requirements) and `transaction-provenance` (1 requirement) over 500 characters.

## Backend

- Fresh container `postgres:16`, migrated by `flyway/flyway:10`: "Successfully applied 39 migrations … now at version v39".
- Full suite on the working tree: **1221 passed**, 0 skipped, 0 failed (+14: 11 in `test_cash_flow_top_categories.py`, 2 in `test_public_transaction_contract.py`, 1 in `test_ingest.py`; one assertion added to an existing habit test).
- CI-style run without a database (`cd backend && python -m pytest tests/`, unreachable database URL): **662 passed, 559 skipped**, exit 0.
- Mutation check on `top_categories` (gross instead of net amounts, zero rows kept): 7 of the 9 database tests in the new file fail; restored afterwards.

## Frontend

- `npm ci` exit 0; `npm test` **56 files, 305 tests** passed (−6 contrast cases for the removed `--warning`, `--info` and `--color-accent-ink` tokens, +1 habit chip test); `npm run lint` exit 0 with 1 warning that is already on origin/main (`review/page.tsx` 111, `_done` unused); `npm run type-check` exit 0; `npm run build` exit 0.
- The new habit chip test fails with the component change reverted and passes with it.
- Token scan (references in `tokens.css`, `globals.css`, `tailwind.config.js` and non-test sources): before the change, 6 unused tokens; after it, the same 6 and no other.
- Built CSS (`.next/static/css`): no rule for `.shadow-xs` or `.shadow-2xs` (Tailwind 3.4.17); `.shadow-sm` and `.shadow-lg` present.

## Browser (headless Brave over CDP, own ports 8631, 3631 and 9631)

uvicorn on a separate migrated database with AI, background jobs and plan auto-linking off; `next dev` with `BACKEND_URL`. A synthetic owner was seeded through the API: three answers teach a habit, a correction makes it 3 of 4, and the next payment is left open on Tinjau.

| Width | Theme | Accessible name of the habit chip | Basis contrast | Keyboard | Overflow | axe (card) |
| --- | --- | --- | --- | --- | --- | --- |
| 390 | light | "Sepertinya Makan Siang Contoh? Makan Siang Contoh · Kebutuhan 3 dari 4 jawaban sebelumnya" | 6.72:1 at 11 px | reached by Tab, `:focus-visible`, 2 px coral outline | none | 0 violations |
| 1280 | light | same | 6.72:1 | same | none | 0 |
| 390 | dark | same | 6.60:1 | same | none | 0 |
| 1280 | dark | same | 6.60:1 | same | none | 0 |

Only the habit chip shows a basis. Console errors: 0. Both servers and the browser were stopped afterwards; the ports were free.

## Compose

- `docker compose -f docker-compose.yml config --quiet` exit 0 (warnings only for unset `INVITE_CODE` and `SESSION_SECRET`).
- Rendered value: `PLAN_AUTO_LINK_ENABLED: "true"` by default, `"false"` with `PLAN_AUTO_LINK_ENABLED=false`.

## OpenSpec and diff

- `openspec validate finish-small-follow-ups --strict` exit 0; `openspec validate --all` exit 0 (64 passed) before the archive.
- `git diff --check` exit 0.
- Final diff review: no secrets, no real names or notification text, no dead code left by the deletions, no unrelated change.

## Archive

- `openspec archive finish-small-follow-ups --yes`: + 5 added, ~ 7 modified, − 0 across `default-pocket`, `financial-reporting-consistency`, `habit-learning`, `idempotent-public-transactions`, `observed-insights`, `tactile-pulse-ui` and `transaction-provenance`. No other open change touched these requirements.
- After the archive, `openspec validate --all --strict` exit 0 (63 passed, 0 warnings).
- Main-spec names went from 374 requirements and 995 scenarios to 379 and 1011. The comparison shows additions only: the 5 new requirements with their 13 scenarios and 3 new scenarios in modified requirements. No requirement or scenario name was removed.

## Not verified

- The Android companion reading `top_categories` (built in parallel, outside this repository).
- Production behaviour and data; release is the owner's pipeline step.
