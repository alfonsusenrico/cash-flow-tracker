# Verification

All checks ran locally on 2026-11-02 against the working tree of `fix/classification-and-green-contrast`, branched from origin/main 3b3d7ef, with this change applied. Only synthetic data and disposable databases were used. There was no production access, no phone and no real AI call; the typed-capture test mocks the provider.

## Backend

- Environment: a Python 3.12.12 venv at the worktree root, built from `backend/requirements-dev.txt` (pytest 9.0.3).
- Database: container `cft-gr-db` (postgres:16, port 55711). `ledger_test` was freshly created and migrated V1→V41 by `flyway/flyway:10` with the flags of `checks.yml`: "Successfully applied 41 migrations ... now at version v41". Environment: `TEST_DATABASE_URL`, `SKIP_TEST_MIGRATIONS=1`, `REQUIRE_TEST_DATABASE=1`.
- `python -m pytest tests/ -ra`: **1418 passed**, 0 failed, 0 skipped, 1 warning (passlib `crypt` deprecation). An earlier run on a fresh database, before the model-pillar test was added, gave 1417 passed.
- New tests in `tests/test_kakeibo_defaults.py`: 4 tests, 5 cases. With the application changes stashed, 3 fail:
  - `test_category_edit_with_is_primary_keeps_the_stored_pillar[True]`: `(True, 'need', 1)`;
  - `test_category_edit_with_is_primary_keeps_the_stored_pillar[False]`: `(False, 'want', 1)`;
  - `test_movement_fee_in_a_fee_category_without_a_pillar_stays_unclassified`: `'need' is None`.

  The model-pillar test asserts unchanged behaviour (D1).

## Frontend

- `npm ci`: 585 packages added. npm 12 blocked the install scripts of `esbuild`, `fsevents`, `sharp` and `unrs-resolver` because they are not covered by `allowScripts`. Test, lint, type-check and build passed regardless.
- `npm test`: **58 files, 337 tests passed**. `tokens.test.ts`: 29 tests. 6 fail on the previous `tokens.css`, `tailwind.config.js` and pages:
  - the emerald mapping;
  - the button source check;
  - `--income-text` and `--income-contrast`, in each theme.
- `npm run lint`: 0 errors and 1 warning. The warning predates this change: `_done` unused in `src/app/review/page.tsx`.
- `npm run type-check`: exit 0.
- `npm run build`: exit 0. The built CSS has:
  - `.text-emerald-400,.text-emerald-500,.text-emerald-600,.text-emerald-700{color:var(--income-text)}`;
  - `.text-income{color:var(--income-text)}`;
  - `.text-income-contrast{color:var(--income-contrast)}`;
  - `.bg-emerald-500{background-color:var(--color-income)}` and `.bg-income{background-color:var(--color-income)}`.

## Browser (headless Brave, CDP port 55714)

Setup:
- `next dev` on 127.0.0.1:55713, and the API on 127.0.0.1:55712, on `ledger_browser` (V41). AI and background jobs were disabled.
- One synthetic owner registered through the API, with three incomes and five expenses in the current month.
- Pages `/`, `/ledger` and `/insights` at 390 × 844 (mobile) and 1280 × 900, in light and dark: 12 loads before the change and 12 after. The rendered theme matched the requested theme on all 24 loads.

Measurements:
- axe ran with the WCAG 2 A/AA tags.
- A probe measured every visible element with its own text whose colour is emerald (hue 140–175°), and every text painted on an emerald fill. Each was measured against its background composited up the ancestor chain.

| | Before | After |
| --- | --- | --- |
| axe `color-contrast` nodes, all 12 loads | 37 | 1 (the amber "Pindah Saldo" button, not green) |
| axe `color-contrast` nodes on emerald text or fills | 36 | 0 |
| Emerald texts measured | 76 | 76 |
| Below 4.5:1 | 38 | 0 |
| Lowest, light | 3.17:1 (income amount on `#E8ECEF`) | 5.16:1 (dark ink on the `#059669` button) |
| Lowest, dark | 2.54:1 (white on the `#10B981` button) | 7.44:1 (`#10B981` text on `#0E1118`) |

Per page:
- `/` light: 4 nodes at 390 px and 6 at 1280 px before ("+Rp …" amounts, "Pemasukan", "+ Catat", "Kelola →", "Lihat →"). 0 after. Dark had none before or after.
- `/ledger`:
  - Light: 6 nodes at 390 px and 10 at 1280 px before (amounts, "Uang Masuk" badges, "8 Total Transaksi", "Catat Transaksi", the active "Semua" chip). 0 after.
  - Dark at 1280 px: 2 before ("Catat Transaksi" and "Semua", white on `#10B981` at 2.53:1). The probe also found the "N" key hint at 3.42:1. 0 after.
- `/insights`: 2 nodes on every load before (the "Tambah Kategori" label and the "Tambah Kategori Pemasukan" button; 3.76:1 light, 2.53:1 dark). 0 after. The two emerald buttons now measure 5.16:1 (light) and 7.66:1 (dark).
- Not contrast, unchanged by this change: axe `svg-img-alt` on the recharts donut on `/` at 1280 px (5 nodes per theme, before and after).
- Horizontal overflow: 0 px on all 24 loads. Console errors: 0 on all 24 loads.
- Screenshots: full-page PNGs `<page>-<width>-<theme>.png` in the session scratchpad folders `green/before/` and `green/after/`. They hold synthetic data only and are not committed. The full-page capture at 1280 px repeats the fixed sidebar oddly in dark; that is a capture artefact, not a layout change.

## Repository

- `git diff --check`: exit 0, including the new change files.
- `openspec validate --all --strict`:
  - Before archiving: 69 passed, 0 failed, exit 0.
  - After `openspec archive`, two merged requirement texts (`kakeibo-classification-completeness`, `movement-pairing`) exceeded the 500-character strict limit. Their added sentences were shortened in the main specs and in the archived deltas. The run then gave 68 passed, 0 failed, exit 0.
  - `mobile/openspec` is not affected; the branch changes nothing under `mobile/`.

## Not verified

- Hover and focus states in the browser. The hover fills were computed: dark ink on the `bg-income/90` hover measures 5.78:1 or better in light and 6.30:1 or better in dark.
- Dialogs and the filter drawer ("Terapkan") were not opened in the browser. They use the same utilities, which the build output and the source check cover.
- Android at runtime. `mobile/` was reviewed as source only, and no phone was used.
- A production build behind the real reverse proxy. The browser check used `next dev`.
