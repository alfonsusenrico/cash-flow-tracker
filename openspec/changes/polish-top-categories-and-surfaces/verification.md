# Verification — 2026-10-31

The branch `fix/uncategorised-shadow-transfer` was cut from origin/main 15dbf61. All checks ran locally on synthetic data. No production host, phone, or real AI call was used.

## Baseline (15dbf61)

- Backend: **1221 passed** on a fresh PostgreSQL 16 database (V1→V39), run on the clean worktree before any change. The CI-style run without a database, on a `git archive` export: **662 passed, 559 skipped**.
- Frontend: `npm ci` exit 0. `npm test`: **56 files, 305 tests** passed.
- `openspec validate --all --strict`: exit 0, 63 passed.
- Live API on the seeded browser owner: `outflow` was 760.000. `top_categories` listed Belanja Dapur 350.000, Kafe 120.000 and Olahraga 90.000, a sum of 560.000. The 200.000 spent without a category was missing.

## Backend

- A freshly recreated `postgres:16` container was migrated by `flyway/flyway:10`: "Successfully applied 39 migrations … now at version v39".
- Full suite on the working tree: **1229 passed**, 0 skipped, 0 failed. The +8 come from `test_cash_flow_top_categories.py`, which went from 11 to 19 tests: 7 database tests and 1 unit test were added, and 1 unit test was rewritten.
- CI-style run without a database (`cd backend && python -m pytest tests/`, with an unreachable `TEST_DATABASE_URL`): **663 passed, 566 skipped**, exit 0.
- Regression check: the new test file run against the 15dbf61 `report_figures.py` failed **8** tests and passed 11. The 8 were the 2 unit tests, the ranked entry, the exact sum for 1, 2 and 3 groups, reimbursements and tenant isolation. The "never exceeds" test passes on both versions, because it pins an invariant that held before.
- Live API after the change, on a re-seeded owner: `outflow` 760.000. `top_categories` listed Belanja Dapur 350.000, `{"category_id": null, "name": "Tanpa kategori", "kakeibo_type": null, "outflow": 200000}` and Kafe 120.000. Olahraga 90.000 was cut by the limit.

## Android companion 2.3.3 (read-only)

- Source: `financial-tracker-mobile-listener` main 4e0d3e6, `versionName` 2.3.3, `versionCode` 25. It was exported with `git archive` into the scratch folder. The owner's checkout was not modified, and no device was used.
- Code reading:
  - `TopCategory.categoryId` is `String?` and is not checked by `shownTopCategories()`.
  - A null `kakeibo_type` is allowed.
  - `CategoryRow` renders the name, amount and bar, and adds a pillar line only for a known pillar.
- Probe (`./gradlew --offline testDebugUnitTest`, Robolectric SDK 34), with a summary carrying `{"category_id":null,"name":"Tanpa kategori","kakeibo_type":null,"outflow":100000}` and one categorised entry:
  - `isComplete()` is true.
  - `shownTopCategories()` returns both entries, with a null `categoryId` and pillar.
  - The stored-JSON round trip keeps them.
  - The rendered `HomeScreen` shows "KATEGORI TERATAS", "Tanpa kategori" and "Rp100.000" next to the categorised row.
  - Result: 2 probe tests passed. The existing `TrackerOptionalFieldsTest` (9) and `TopCategoriesUiTest` (36) also passed on the same copy.
- Outcome: 2.3.3 does **not** hide the tile. It shows "Tanpa kategori" with its amount and bar and no pillar line.

## Frontend

- `npm ci` exit 0. `npm test`: **56 files, 308 tests** passed (+3 in `tokens.test.ts`). `npm run lint`: exit 0, with 1 warning that is already on origin/main (`review/page.tsx` 111, `_done` unused). `npm run type-check`: exit 0. `npm run build`: exit 0.
- The 3 new token tests fail with the 15dbf61 `tailwind.config.js` and `tokens.css`, and pass with the change.
- Built CSS (`.next/static`) has `.shadow-xs{--tw-shadow:var(--shadow-xs);…}`. No rule references `--color-transfer`.
- `rg` for `color-transfer`, `transfer-soft`, `transfer-hover`, `chartColors.transfer` and `chartColors.accent` finds nothing in `frontend/src`, `tokens.css`, `tailwind.config.js` or `openspec/specs`.

## Browser (headless Brave over CDP; ports 8641, 3641 and 9641)

uvicorn ran on its own migrated database, with AI, background jobs and plan auto-linking off. `next dev` ran with `BACKEND_URL`. A synthetic owner was seeded through the API. It had two accounts, an income, three categorised expenses, one uncategorised expense, a movement, a goal and an obligation.

The run covered 7 pages (Beranda, Transaksi, Analisis, Tinjau, Rekening & Saldo, Target & Tagihan, Settings), at 390 and 1280 px, in light and dark, before and after. That is 28 runs each time.

| | Before | After |
| --- | --- | --- |
| Rendered `shadow-xs` elements (sum over 28 runs) | 160 | 160 |
| … with `box-shadow: none` | 122 | 0 |
| … with `--shadow-xs` (light `rgba(8, 10, 14, 0.04) 0 1px 2px 0`, dark `rgba(0, 0, 0, 0.3) 0 1px 2px 0`) | 0 | 122 |
| … with an existing heavier shadow (`.card-squircle`/`.cockpit-card` `--shadow-sm`, one `--shadow-md`) | 38 | 38, identical values |
| `shadow-2xs` elements | unchanged | unchanged (still no shadow) |
| Horizontal overflow | 0 px on every run | 0 px on every run |
| Console errors | 0 | 0 |
| Ledger transfer chip colour (1280 px) | `rgb(14, 165, 233)` | `rgb(14, 165, 233)` |

- Settings has no `shadow-xs` element.
- Zoomed crops of the Beranda Kakeibo cards show a one-pixel soft lower edge after the change, in light and dark. The 1px border is still the visible separator, and no glow or heavy shadow appears.
- Full-page screenshots for all 28 runs, before and after, are in the scratch folder (`polish/browser/before`, `polish/browser/after`). They are not committed, because they show seeded test data.
- Both servers and the browser were stopped afterwards, and the ports were free.

## OpenSpec and diff

- `openspec validate polish-top-categories-and-surfaces --strict`: exit 0. `openspec validate --all --strict`: exit 0, 64 passed, before the archive.
- `git diff --check`: exit 0.
- Final diff review: no secrets, no real names or notification text, and no dead code. Only the three approved items changed, plus their tests and specs.

## Not verified

- An installed phone. The 2.3.3 behaviour comes from code reading and a JVM/Robolectric probe on its source, not from a device.
- Production behaviour and data. Release is the owner's pipeline step.
