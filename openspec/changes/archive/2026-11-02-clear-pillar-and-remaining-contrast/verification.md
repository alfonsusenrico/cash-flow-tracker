# Verification

All checks ran locally on 2026-11-02 against the working tree of `fix/clear-pillar-and-contrast`, branched from origin/main e05cde9, with this change applied. Only synthetic data and disposable databases were used. There was no production access, no phone and no AI call.

## Backend

- Environment: a Python 3.12.12 venv at the worktree root, built from `backend/requirements-dev.txt` (pytest 9.0.3).
- Database: container `cft-cl-db` (postgres:16, port 55721). `ledger_test` was freshly created and migrated V1→V41 by `flyway/flyway:10` with the flags of `checks.yml`: "Successfully applied 41 migrations ... now at version v41". Environment: `TEST_DATABASE_URL`, `SKIP_TEST_MIGRATIONS=1`, `REQUIRE_TEST_DATABASE=1`.
- `python -m pytest tests/ -ra`: **1423 passed**, 0 failed, 0 skipped, 1 warning (passlib `crypt` deprecation). The previous change's run gave 1418; the difference is the five new tests.
- New tests in `tests/test_kakeibo_defaults.py`: 5. On the previous `routers/categories.py`, 4 fail:
  - `..._explicit_null_pillar_clears_it_and_writes_feedback`: 400 "No fields to update";
  - `test_category_form_choosing_unclassified_clears_the_pillar`: `('want', 0) == (None, 1)`;
  - `test_clearing_another_owners_pillar_is_not_found_and_changes_nothing`: 400 instead of 404;
  - `test_income_category_form_with_null_pillar_and_budget_saves`: `psycopg.errors.SyntaxError: multiple assignments to same column "monthly_budget"`. This is the HTTP 500 that the web form hit on every income-category edit (design.md D2).

  `..._without_the_pillar_field_keeps_the_stored_pillar` passes before and after, because keeping is unchanged.

## Frontend

- `npm ci`: 585 packages added. npm blocked the install scripts of `esbuild`, `fsevents`, `sharp` and `unrs-resolver` because they are not covered by `allowScripts`. Test, lint, type-check and build passed regardless.
- `npm test`: **59 files, 347 tests passed**. 10 of the new or extended tests fail on the previous sources:
  - 8 in `tokens.test.ts` (37 tests): the amber mapping, the Target & Tagihan source check, the tile check, the donut label check, and `--warning-text` and `--income-hover-contrast` in each theme;
  - the donut figure test;
  - the "Belum diklasifikasi" form test.
- `npm run lint`: 0 errors and 1 warning. The warning predates this change: `_done` unused in `src/app/review/page.tsx`.
- `npm run type-check`: exit 0.
- `npm run build`: exit 0. The built CSS has:
  - `.text-amber-500,.text-amber-600,.text-amber-700{color:var(--warning-text)}`;
  - `.text-income-hover-contrast{color:var(--income-hover-contrast)}`;
  - `.bg-amber-500{... rgb(245 158 11 ...)}` and `.bg-emerald-600,.bg-emerald-700{background-color:var(--color-income-hover)}`, so the fills are unchanged.

## Browser (headless Brave, CDP port 55724)

Setup:
- `next dev` on 127.0.0.1:55723, and the API on 127.0.0.1:55722, on `ledger_browser` (V41). AI and background jobs were disabled.
- One synthetic owner registered through the API, with the starter categories, two cash accounts, three incomes and five expenses in the current month, and no goals.
- Views:
  - `/`;
  - `/goals` (Target & Tagihan, empty, so "Target Baru" and "Buat Target Pertama Anda" both show);
  - the Analisis category form, opened on an expense category.
- Each view at 390 × 844 (mobile) and 1280 × 900, in light and dark: 12 loads before the change (frontend sources stashed) and 12 after. The rendered theme matched the requested one on all 24 loads.

Measurements:
- axe ran with the WCAG 2 A/AA tags.
- A probe measured the fixed items against their background composited up the ancestor chain.

| | Before | After |
| --- | --- | --- |
| axe violations, all 12 loads | 15 nodes: `color-contrast` 5, `svg-img-alt` 10 | 0 |
| `/` 1280 light: "Pindah Saldo" text button | 3.19:1 (`#D97706` on white), axe `color-contrast` 1 | 6.39:1 (`#8F4F04`) |
| `/` 1280 light: "Pindah Saldo" capsule (`text-amber-700`) | 4.65:1 | 5.92:1 |
| `/` 1280 dark: both "Pindah Saldo" | 11.31:1 and 9.77:1 | unchanged |
| `/goals` dark, 390 and 1280: "Target Baru" and "Buat Target Pertama Anda" | 1.92:1 (white on `#34D399`), axe 2 nodes per load | 10.11:1 (`#0A0D14`) |
| `/goals` light: the same buttons | 5.48:1 | 5.48:1 (white kept) |
| `HeroWalletCard` emerald tile icon, light / dark | 3.77:1 / 2.54:1, not hidden | 5.16:1 / 7.66:1, `aria-hidden="true"` |
| `/` 1280 donut | 5 sectors, 5 unnamed, axe `svg-img-alt` 5 per theme | 5 sectors, 0 unnamed, figure "Diagram donat Pengeluaran per Kategori" with a summary of all 5 |
| Category form pillar options | 5 pillars, no "Belum diklasifikasi" while the category has one | "Belum diklasifikasi" + 5 pillars |

Other results:
- At 390 px, Beranda shows the phone home view, without the donut or the wallet card. axe found nothing there before or after.
- End to end, once after the 12 loads, at 1280 px:
  1. The category form was opened on "Makanan & Minuman" (`need`).
  2. "Belum diklasifikasi" was chosen by keyboard (Home on the focused select) and the form was saved.
  3. `GET /api/categories` then returned `kakeibo_type: null` for it.
  4. `classification_feedback` held one `correction` row, `need` to null, for its "Warung Contoh" expense.
- Horizontal overflow: 0 px on all 24 loads. Console errors: 0 on all 24 loads.
- Screenshots: `<view>-<width>-<theme>.png` in the session scratchpad folders `clear-pillar/before/` and `clear-pillar/after/`. They are full-page for `/` and `/goals`, and the viewport for the open form. They hold synthetic data only and are not committed.

## Repository

- `git diff --check`: exit 0, including the new change files.
- `openspec validate --all --strict`, exit 0 each time:
  - before archiving: 69 passed, 0 failed;
  - after `openspec archive`: 68 passed, 0 failed.

  `mobile/openspec` gives 9 passed, 0 failed. The branch changes nothing under `mobile/`.

## Not verified

- The "Simpan Progres" dialog on Target & Tagihan was not opened in the browser. It uses the same token, which the source check and the token pair cover.
- Hover states in the browser. The hover fills of the changed buttons resolve to the same fill (`bg-emerald-700` = `bg-emerald-600`), and amber text changes no hover.
- Amber text on pages outside the three views (for example, Analisis "Waspada" badges, the recurring banner). It is covered by the token pairs and the built CSS, not by a browser measurement.
- Screen-reader output of the donut summary. Only its accessible name and description were checked, in the DOM and in jsdom.
- Android at runtime. `mobile/` was reviewed as source only, and no phone was used.
- A production build behind the real reverse proxy. The browser check used `next dev`.
