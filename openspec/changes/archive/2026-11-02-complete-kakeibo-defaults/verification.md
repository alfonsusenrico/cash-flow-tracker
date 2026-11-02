# Verification

All checks ran locally on 2026-11-02 against the working tree of `feat/monthly-report`: merge commit 21cd148 plus this change. Only synthetic data and disposable databases were used. There was no production access and no real AI call.

## Branch update

- `git merge origin/main` (7fcd266). It had two conflicts:
  - `AGENTS.md`: both sides were kept.
  - `backend/app/services/text_capture.py`: the pillar SQL keeps no `need` fallback and selects `time_basis` and `time_evidence`.
- `db/migrations` holds one file per version, with no duplicates. Flyway 10 migrated a fresh PostgreSQL 16 database V1→V41: "Successfully applied 41 migrations ... now at version v41".

## Backend

- Environment: a Python 3.12.12 venv at the worktree root, built from `backend/requirements-dev.txt` (pytest 9.0.3, httpx 0.28.1, pytest-asyncio 1.4.1.post1).
- Database: container `cft-rep2-db` (postgres:16), `ledger_test` freshly migrated V1→V41 by Flyway with the CI flags. Environment: `TEST_DATABASE_URL`, `SKIP_TEST_MIGRATIONS=1`, `REQUIRE_TEST_DATABASE=1`.
- `python -m pytest tests/ -ra`: **1414 passed**, 0 failed, 0 skipped, 1 warning (passlib `crypt` deprecation).
- New `tests/test_kakeibo_defaults.py`: 12 tests. With the application changes stashed, 8 fail and 4 pass. The 4 that pass are the owner-scope, explicit-pillar, owner-chosen capture pillar and existing-user tests, which assert unchanged behaviour.
- Loopback API run on `ledger_browser`:
  - a registration listed the starter pillars of design.md D4;
  - `PATCH` "Hiburan" to `want` returned `corrections: 1`, and a repeat returned 0;
  - `POST /api/categories` without a pillar returned `kakeibo_type: null`;
  - `classification_feedback` held one `correction` row (null → want).

## Frontend

- `npm ci`: 585 packages added.
- `npm test`: **58 files, 331 tests passed**. The token test fails at 2.20:1 (light) and 2.60:1 (dark) when `--dock-muted` is set back to `#4E5D70`.
- `npm run lint`: 0 errors and 1 warning. The warning predates this change: `_done` unused in `src/app/review/page.tsx`.
- `npm run type-check`: exit 0.
- `npm run build`: exit 0.

## Browser (headless Brave, CDP port 55704)

Setup: `next dev` on 127.0.0.1:55703 and the API on 127.0.0.1:55702. The owner was seeded by registration, with "Hiburan" and "Belanja" reset to no pillar to stand in for pre-default categories. The viewport was 390 × 844, with `/laporan`, `/`, `/ledger` and `/insights` each checked in light and dark.

- The dock fill is `rgba(23, 26, 33, 0.94)` in both themes. Inactive labels are `rgb(139, 154, 175)`, measuring 5.17:1 composited over white, and 5.25:1 (light) or 6.16:1 (dark) over the page. The active pill measures 8.83:1.
- axe `color-contrast` scoped to the dock: 0 violations on all 8 page and theme runs.
- `/laporan`: 0 axe violations in light and dark, 0 px horizontal overflow and 0 console errors. Before this change, axe reported `color-contrast` on the dock labels here.
- Other pages: axe still reports `color-contrast` outside the dock. These findings predate this change (design.md, Follow-ups): `/` light has 3 (`text-emerald-500`), `/ledger` light has 7 (`text-emerald-500`), and `/insights` has 2 in both themes (the `bg-emerald-500` button).
- Overflow was 0 px and console errors 0 on every run.
- Screenshots: `laporan-390-{light,dark}.png`, `beranda-390-{light,dark}.png` and their `-dock.png` crops, kept in the session scratchpad, not in the repository. The Next.js dev indicator ("N") overlaps the dock in dev mode only.

## Repository

- `git diff --check`: exit 0.
- `openspec validate --all --strict`: 69 passed, 0 failed, exit 0. `mobile/openspec` is not affected; the branch changes nothing under `mobile/`.

## CI

- Checks run 41391455053 on dfff3a4: all three jobs succeeded. The backend job migrated to schema version 41 and reported 1414 passed. The frontend job passed type check, lint (the same pre-existing warning) and build. The compose and backup round-trip job passed.

## Not verified

- Android runtime with a null pillar. That was checked by source review only (design.md D3), and no phone was used.
- A production build served behind the real reverse proxy. The browser check used `next dev`.
