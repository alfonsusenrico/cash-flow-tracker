# Tasks

## 1. Clearing a pillar

- [x] 1.1 D1 and D2: `PATCH /api/categories/{id}` clears the pillar on an explicit `kakeibo_type: null`, keeps it when the field is left out, writes feedback through the existing pillar-change path, and assigns each column once. Evidence: the five new tests in `tests/test_kakeibo_defaults.py`. On the previous code, four fail:
  - clear: 400 "No fields to update";
  - form shape: `('want', 0)`;
  - another owner: 400 instead of 404;
  - income form: `multiple assignments to same column "monthly_budget"`.

  All five pass now.
- [x] 1.2 D3: the web category form always offers "Belum diklasifikasi" and sends an explicit null when it is chosen. Evidence: the `app/insights/page.test.tsx` case, which fails on the previous page and passes now, and the browser run: chosen by keyboard, saved, listed with `kakeibo_type: null`, one feedback row `need` to null.
- [x] 1.3 D1: review every client that writes categories, with `mobile/` read only. Evidence: the client list in design.md D1.

Expected Result: an owner can clear a category's pillar, habit learning learns from it, and no client clears a pillar it did not mean to.

## 2. Remaining contrast

- [x] 2.1 Extend `frontend/src/app/tokens.test.ts`:
  - `--warning-text` and `--income-hover-contrast` pairs in both themes;
  - the amber mapping;
  - the pinned fills;
  - source checks for Target & Tagihan, the `HeroWalletCard` tiles and the donut labels.

  Evidence: 8 of 37 fail on the previous tokens, config and components, and 37 of 37 pass now.
- [x] 2.2 D4 and D5: add `--warning-text` and `--income-hover-contrast`, map `text-amber-400` to `text-amber-700`, and use `text-income-hover-contrast` on the three `bg-emerald-600` buttons. Evidence: the built CSS has `.text-amber-500,.text-amber-600,.text-amber-700{color:var(--warning-text)}` and `.text-income-hover-contrast{color:var(--income-hover-contrast)}`. `.bg-amber-500` and `.bg-emerald-600,.bg-emerald-700` keep their fills.
- [x] 2.3 D6: mark the `HeroWalletCard` icon tiles decorative, and give the emerald tile's icon `text-income-contrast`. Evidence: the browser probe, 5.16:1 light and 7.66:1 dark.
- [x] 2.4 D7: name and summarise the donut, and label every sector. Evidence: `CategoryDonutChart.test.tsx`, and axe `svg-img-alt` going from 5 nodes to 0 in each theme at 1280 px.

Expected Result: the four reported items measure at least 4.5:1 for text and 3:1 for the icon in both themes, axe reports none of them, and every fill keeps its colour and meaning.

## 3. Verification and delivery

- [x] 3.1 Backend suite on a fresh PostgreSQL 16 database migrated V1→V41 by `flyway/flyway:10`, with `SKIP_TEST_MIGRATIONS=1` and `REQUIRE_TEST_DATABASE=1`, in a Python 3.12 venv from `backend/requirements-dev.txt`. Evidence: counts in verification.md.
- [x] 3.2 Frontend `npm ci`, test, lint, type-check and build. Evidence: counts in verification.md.
- [x] 3.3 Headless Brave on `/`, `/goals` and the Analisis category form at 390 and 1280 px, in light and dark, before and after. Evidence:
  - axe;
  - the measured items;
  - overflow;
  - console errors;
  - screenshots.
- [x] 3.4 `git diff --check` and `openspec validate --all --strict`. Evidence: exit 0.
- [x] 3.5 Archive the change, push `fix/clear-pillar-and-contrast` and report the Checks workflow result. Evidence: the pushed branch and the Checks run.

Expected Result: local checks pass, the change is archived, the branch is pushed, and CI reports its result.
