# Tasks

## 1. Top-ups in the home feed

- [x] 1.1 D1: join `investment_topups` in the `recent_transactions` query of `/api/dashboard/overview` and return `investment_topup_id`. Evidence: `tests/test_home_feed_colours.py` finds the top-up id on both legs and `null` on the expense and both "Pindah Saldo" legs, and the same statement count with one and four top-ups. Both tests fail on the previous code.
- [x] 1.2 D2: paint top-up rows and the detail dialog lavender and name them on Beranda and in `MobileHomeView`. Evidence: `app/page.test.tsx` and `MobileHomeView.test.tsx` (4 tests, all failing on the previous components), and the browser run finds lavender top-up rows and sky transfer rows at 390 and 1280 px in both themes.

Expected Result: a top-up in "Aktivitas Terkini" reads as an investment, by colour and by name.

## 2. Lavender default for new investment accounts

- [x] 2.1 D3: default `AccountCreate.color` by type, and colour broker-opened positions. Evidence: `tests/test_home_feed_colours.py` (default by type, explicit colour kept, investment pocket, stored colour kept after a rename) and the colour check in `test_broker_buy_sell_is_deterministic_and_replay_safe`. Six of these fail on the previous code. The browser run shows the seeded investment platform tag in `rgb(139, 92, 246)` and the bank and cash tags still blue.
- [x] 2.2 D3: the Rekening form starts investment accounts on the new "Lavender" swatch and follows the type until the owner picks one. Evidence: two new tests in `accounts/page.test.tsx` and the colour in the existing investment platform test.

Expected Result: new investment accounts are lavender; nothing already stored changes.

## 3. Distinct Kakeibo pillar hues

- [x] 3.1 D4: new `--color-kakeibo-*` values and soft fills in both themes, and removal of the unused `chartColors.cyan` alias. Evidence: the D4 table, and the browser run measures five pillar hues (227, 316, 70, 23, 134 light; 227, 318, 67, 22, 133 dark) on Beranda, Analisis and Laporan.
- [x] 3.2 D5: extend `tokens.test.ts`. Evidence: 9 of 71 fail on the previous tokens and 71 of 71 pass now.

Expected Result: every pillar has its own hue that means nothing else, with readable marks and labels in both themes.

## 4. Verification

- [x] 4.1 Backend: full suite on a fresh Flyway-migrated PostgreSQL 16 (V1 to V42) with `REQUIRE_TEST_DATABASE=1`. Evidence: 1523 passed, 0 skipped.
- [x] 4.2 Web: `npm ci`, type-check, lint, test (`TZ=Asia/Jakarta`) and build. Evidence: exit 0 each; lint reports one existing warning in `review/page.tsx`; 415 tests in 62 files pass.
- [x] 4.3 Headless Brave on `/`, `/insights` ("Bandingkan & Kakeibo"), `/laporan` and `/accounts` at 390 and 1280 px, light and dark, with synthetic data. Evidence in verification.md: 16 screenshots, no overflow, no console errors, no axe violation except the two existing `color-contrast` nodes on `/accounts` at 1280 px.
- [x] 4.4 `git diff --check` and `openspec validate --all --strict`. Evidence: exit 0.

Expected Result: local checks pass on the exact working tree that is committed.

## Workflow follow-up

- Archive the change with `openspec archive refine-investment-and-pillar-colours`.
- Push `fix/colour-followups` and report the compare link. The owner opens and merges the pull request.
