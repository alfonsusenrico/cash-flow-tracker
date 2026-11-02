# Tasks

## 1. Classification

- [x] 1.1 D1: record the confident model pillar on a typed capture as decided, with no code change. Evidence: the spec scenario "Typed capture with a confident model pillar" and `test_typed_capture_keeps_a_confident_model_pillar_for_a_category_without_one`, which uses a mocked provider.
- [x] 1.2 D2: `PATCH /api/categories/{id}` never derives a pillar from `is_primary`, and the web and `mobile/` clients were checked (read only). Evidence: `test_category_edit_with_is_primary_keeps_the_stored_pillar[True/False]` fails on the previous code with `(True, 'need', 1)` and `(False, 'want', 1)`, and passes now. The client review is in design.md D2.
- [x] 1.3 D3: a movement fee in a fee category without a pillar stays unclassified. Evidence: `test_movement_fee_in_a_fee_category_without_a_pillar_stays_unclassified` fails on the previous code (`'need' is None`) and passes now. `test_known_fee_pairs_with_equal_legs_and_a_fee_row` still shows a pairing-created category at `need`.

Expected Result: no category update or movement fee invents a pillar, and the model's confident pillar on a typed capture is kept.

## 2. Green contrast

- [x] 2.1 Extend `frontend/src/app/tokens.test.ts`:
  - `--income-text` and `--income-contrast` pairs in both themes;
  - the emerald Tailwind mapping;
  - the pinned emerald fills;
  - the Analisis and Transaksi button source check.
  Evidence: 6 of 29 fail on the previous tokens, config and pages, and 29 of 29 pass now.
- [x] 2.2 Add `--income-text` and `--income-contrast` to `tokens.css`. Map `text-emerald-400` to `text-emerald-700` and `text-income` to the ink, and add `text-income-contrast`. Evidence: the built CSS has `.text-emerald-400,.text-emerald-500,.text-emerald-600,.text-emerald-700{color:var(--income-text)}`, `.text-income{color:var(--income-text)}`, `.text-income-contrast{color:var(--income-contrast)}` and `.bg-emerald-500{background-color:var(--color-income)}`.
- [x] 2.3 Use `text-income-contrast` on the emerald buttons of Analisis and Transaksi, and keep their hover and key hint at 4.5:1 or better. Evidence: headless Brave and axe, before and after, in verification.md.

Expected Result: green text and text on green fills reach 4.5:1 in both themes, and every emerald fill is unchanged.

## 3. Verification and delivery

- [x] 3.1 Backend suite on a fresh PostgreSQL 16 database migrated V1→V41 by `flyway/flyway:10`, with `SKIP_TEST_MIGRATIONS=1` and `REQUIRE_TEST_DATABASE=1`, in a Python 3.12 venv from `backend/requirements-dev.txt`. Evidence: counts in verification.md.
- [x] 3.2 Frontend `npm ci`, test, lint, type-check and build. Evidence: counts in verification.md.
- [x] 3.3 Headless Brave on `/`, `/ledger` and `/insights` at 390 and 1280 px, in light and dark, before and after. Evidence:
  - axe `color-contrast`;
  - measured emerald text;
  - overflow;
  - console errors;
  - screenshots.
- [x] 3.4 `git diff --check` and `openspec validate --all --strict`. Evidence: exit 0.
- [x] 3.5 Archive the change, push `fix/classification-and-green-contrast` and report the Checks workflow result. Evidence: the pushed branch and the Checks run.

Expected Result: local checks pass, the change is archived, the branch is pushed, and CI reports its result.
