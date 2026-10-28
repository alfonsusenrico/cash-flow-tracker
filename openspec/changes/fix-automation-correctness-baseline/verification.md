# Verification: fix-automation-correctness-baseline

Date: 2026-10-28.

**Revision tested:** branch `fix/automation-correctness-baseline`, based on `origin/main` fab3811. The working tree was uncommitted when tested; the commit that follows contains exactly that tree.

**Data:** synthetic users, accounts, categories and notification texts only.

## Backend

| Check | Result |
|---|---|
| Disposable `postgres:16`, migrated V1→V27 by `flyway/flyway:10` from a dropped and recreated database | 27 migrations applied |
| Full suite: `PYTHONPATH=backend pytest backend/tests` with `SKIP_TEST_MIGRATIONS=1` | **742 passed, 0 skipped, 0 failed** |
| Of which new | 26 tests across `test_automation_correctness_database.py`, `test_notification_noise_rules.py` and `test_notification_processing_database.py` |
| Existing tests before any new test was added | 716 passed (one fake-cursor test needed `row.get`, matching neighbouring code) |
| New noise tests against `origin/main` code | 13 of 14 fail; the unchanged USD-review case passes |
| New database tests against `origin/main` code on the same database | 6 of 9 fail; the 3 that pass are the constraint and mirror tests (the database is already at V27) and "follows category", which old code already did |
| Live API on the test database (`uvicorn`, AI disabled), synthetic cycle with Rp6.000.000 income | `kakeibo`: need 23.3, want 10.8, culture 5.0, unexpected 7.5, saving 13.3, essentials 30.8, lifestyle 15.8, status `healthy`. `pulse.today_spent` 2.800.000, which excludes the saving-pillar Rp800.000. All values match hand calculation. |

**Noise-rule prototype** over 349 real notifications, run read-only in a scratch area outside the repository; texts were not stored in the repository:
- the input was every stored production event plus every row of the owner's phone database;
- 0 recorded transactions would be newly ignored;
- 15 of 16 open production review items move to ignored;
- the remaining item is a real broker fund-release event, which stays in review.

## Frontend

| Check | Result |
|---|---|
| `npm test` | 36 files, **163 passed** (148 before plus 15 new) |
| `npm run lint` | passed |
| `npm run type-check` | passed |
| `npm run build` | compiled successfully |
| axe on `KakeiboBenchmark` (vitest-axe) | 0 violations |

**Browser check:** headless Brave over DevTools, with a throwaway profile, against the local Next dev server and backend.

| Width | Theme | Views | Result |
|---|---|---|---|
| 390 px | light, dark | Insights Kakeibo | No horizontal overflow; badge "Kakeibo sehat"; five legend entries; three group cards |
| 1280 px | light, dark | Insights Kakeibo, home allocation cards, metric tile | No horizontal overflow; group labels fit after the short-label fix |

- Theme followed the emulated `prefers-color-scheme`; the `dark` class was confirmed.
- Console errors: 0.
- **Keyboard**, quick capture at 390 px:
  - pillar buttons tab in the order Kebutuhan > Keinginan > Budaya > Tak Terduga > Tabungan;
  - Space selects (`aria-pressed=true`);
  - the focused button matches `:focus-visible` with a visible outline.
- **Contrast**, WCAG 1.4.11 non-text, for pillar fills and dots against the paper, surface and raised surface of each theme. New tokens:

  | Token | Theme | Lowest ratio |
  |---|---|---|
  | culture `#0369A1` | light | 4.41 |
  | unexpected `#B45309` | light | 3.73 |
  | culture `#38BDF8` | dark | 7.79 |
  | unexpected `#FBBF24` | dark | 10.00 |

## Operations

| Check | Result |
|---|---|
| `docker compose --env-file .env.example config -q` | passed. Services: db, migrate, redis, api, frontend, web |
| `nginx -t` in `nginx:alpine` with the new `default.conf` | passed |
| `.env.example`, read with the keys-only reader | 15 bot keys removed; 40 keys remain |
| `git grep -i -E "telegram\|deepseek\|bot_secret"` | Remaining hits are all historical: the V1 migration and `db/init.sql` comments (migrations are immutable), the design history of in-flight change `add-ai-notification-processing`, research notes from 2026-10-07, and `openspec/specs/clean-core-ledger`, which this change's delta updates on archive |

## OpenSpec and diff

| Check | Result |
|---|---|
| `openspec validate fix-automation-correctness-baseline` | valid, also with `--strict` |
| `openspec validate --all` | 45 passed, also with `--strict` |
| `git diff --check` | clean |

## Not verified, and why

- **200% text resize:** not tested separately. Reflow was checked at 390 px. Many existing labels use fixed pixel sizes project-wide.
- **Live LLM evaluation:** the prompt changed in pillar definitions and the category's default pillar. Live evals need the production OpenAI key and paid usage, which this change did not approve. Offline benchmark and real-corpus tests pass.
- **Production:** not touched. Constraint, backlog, adjustment and bot-container checks are recorded in `PROJECT_STATE.md` for after the merge.
- **Screen reader:** not run. Accessible names and roles were checked with Testing Library queries and axe.

## Follow-ups

- The light theme's existing want (`#FF334B`) and saving (`#059669`) tokens are 2.68 and 2.80:1 on the raised surface, below 3:1. They predate this change.
- The global phone rule in `globals.css` forces `grid-cols-2/3/4/6` into a single column below the phone breakpoint, so the five pillar buttons stack on a phone.
- The bot's database tables (`bot_users`, `bot_chat_history`, `bot_pending_actions`) and the empty `bot_storage` volume remain. Dropping them is an owner decision.
