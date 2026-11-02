# Verification

All runs were local, on 2026-11-02, on the working tree of `fix/colour-followups` (from origin/main 7d9f935) before its commit. Data was synthetic. Nothing touched the production server.

## Backend

- Database: PostgreSQL 16 in the disposable container `cft-colours-pg`, recreated and migrated V1→V42 by `flyway/flyway:10`, as `checks.yml` does.
- `python -m pytest tests/ -ra` with `SKIP_TEST_MIGRATIONS=1` and `REQUIRE_TEST_DATABASE=1`: **1523 passed, 0 skipped** (1513 on origin/main before this change, plus 10 new).
- Negative path: with the previous `backend/app`, 7 of the new or changed tests fail: both home feed tests, the three investment default cases, the investment pocket default and the broker position colour. The other 4 (bank and cash defaults, explicit colour, stored colour kept) pass on both, as they should.

## Frontend

- `npm ci`: exit 0.
- `npm run type-check`: exit 0.
- `npm run lint`: exit 0, 0 errors, 1 warning. The warning is the existing unused `_done` in `src/app/review/page.tsx`, which this change does not touch.
- `TZ=Asia/Jakarta npm test`: **62 files, 415 tests passed** (61 files and 393 tests before; 22 new).
- `src/app/tokens.test.ts` alone: 9 of 71 fail on the previous tokens and 71 of 71 pass now.
- `app/page.test.tsx` and `MobileHomeView.test.tsx`: the 4 home feed tests fail on the previous components and pass now.
- `npm run build`: exit 0.

## Browser

Setup:
- headless Brave over CDP on debug port 9452;
- `next dev` on 127.0.0.1:3152 and the API on 127.0.0.1:8152, against a fresh database `ledger_browser` migrated V1→V42;
- one synthetic owner with salary, expenses in all five pillars and one unclassified, in this month and last month, one "Pindah Saldo" of Rp 200.000, an investment platform and a mutual-fund product created without a colour, and one top-up of Rp 250.000;
- `/`, `/insights` (tab "Bandingkan & Kakeibo"), `/laporan` and `/accounts` at 390 and 1280 px, light and dark: 16 views.

Every view: no horizontal overflow, no console errors. axe (WCAG 2.0 to 2.2 A and AA) reports nothing, except the two `color-contrast` nodes on `/accounts` at 1280 px ("0 cicilan aktif" and the essential-cost note, muted text at 75% opacity, 3.7:1 light and 4.21:1 dark). They predate this change and were recorded as out of scope in `align-investment-and-info-colours`.

Home feed (`/`):

| Width, theme | Top-up amount | Top-up tile hue, icon on tile | Transfer amount |
| --- | --- | --- | --- |
| 390 light | `rgb(91, 33, 182)`, hue 263, 8.98:1 | 258, 7.45:1 | `rgb(7, 89, 133)`, hue 201, 7.56:1 |
| 390 dark | `rgb(167, 139, 250)`, hue 255, 6.94:1 | 250, 5.97:1 | `rgb(56, 189, 248)`, hue 198, 8.82:1 |
| 1280 light | `rgb(91, 33, 182)`, hue 263, 8.98:1 | 256, 7.94:1 | `rgb(7, 89, 133)`, hue 201, 7.56:1 |
| 1280 dark | `rgb(167, 139, 250)`, hue 255, 6.94:1 | 246, 6.33:1 | `rgb(56, 189, 248)`, hue 198, 8.82:1 |

Both legs of the top-up are lavender in each view, and both legs of the transfer stay sky.

Kakeibo pillars:

| View | Pillar hues (need, want, culture, unexpected, saving) | Closest pillars | Closest to sky, lavender, emerald or coral | Lowest label text | Lowest mark |
| --- | --- | --- | --- | --- | --- |
| `/` 1280 light | 227, 316, 70, 23, 134 | 47 | 27 | 6.72:1 | 4.75:1 (11 marks) |
| `/` 1280 dark | 227, 318, 67, 22, 133 | 45 | 27 | 6.60:1 | 6.19:1 (11 marks) |
| `/insights` 390 and 1280 light | 227, 316, 70, 23, 134 | 47 | 27 | 6.72:1 | 6.39:1 (6 marks) |
| `/insights` 390 and 1280 dark | 227, 318, 67, 22, 133 | 45 | 27 | 6.60:1 | 7.01:1 (6 marks) |
| `/laporan` 390 and 1280 light | 227, 316, 70, 23, 134 | 47 | 27 | 19.43:1 | 6.39:1 (6 marks) |
| `/laporan` 390 and 1280 dark | 227, 318, 67, 22, 133 | 45 | 27 | 18.05:1 | 7.01:1 (6 marks) |

The phone-width Beranda lists pillars by name only, without marks, as before. "Belum diklasifikasi" is slate in every view.

Accounts (`/accounts`, all four views): the seeded investment platform tag is `rgb(139, 92, 246)` with 4.96:1 tag text; "Rekening Utama" and "Uang Tunai" stay `rgb(59, 130, 246)`.

## OpenSpec and diff

- `openspec validate --all --strict`: exit 0.
- `git diff --check`: exit 0.
