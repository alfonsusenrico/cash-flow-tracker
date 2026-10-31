# Verification — 2026-10-31

Branch `fix/phone-spacing-tokens` at 6404597 plus this change. All checks ran locally on synthetic data. No production host, phone or real AI was used. This is not a WCAG conformance audit.

## Tests that fail before the fix

- Backend: `test_spending_so_far_includes_days_left_out_of_a_shortened_comparison` and `test_spending_so_far_matches_the_overview_for_the_same_period` failed with `KeyError: 'current_to_date'`.
- Frontend: the two new `NarrativeInsightCard` tests failed on the old card. The new `ConfirmActionButton` test failed on the old component. `tokens.test.ts` failed 9 of 11 on the previous tokens (undefined properties; danger, warning, info and accent pairs).

## Final evidence

- **Frontend:** `npm test` passed 55 files and 291 tests. `npm run lint` exited 0 with the existing `review/page.tsx:111` warning. `npm run type-check` and `npm run build` exited 0.
- **Backend:** a fresh `postgres:16` migrated by `flyway/flyway:10` V1→V38: 1171 passed. Without a database: 639 passed, 532 skipped.
- **Browser setup:** headless Brave over CDP on ports 9371–9374. uvicorn ran on 127.0.0.1:8071 with AI disabled, on a fresh database migrated to V38. `next dev` ran on 127.0.0.1:3071. The seed was one synthetic owner with one bank account, six transactions dated today, two goals and one obligation.
- **Seven pages** at 1440, 1280, 390 and 320 px in light and dark, 56 loads:
  - main padding at phone widths went from `14px 12px 96px` to `14px 16px 96px`;
  - desktop padding and document width were unchanged on all 28 desktop loads;
  - horizontal overflow on 0 loads; elements outside the viewport and not in a scroll container: 0;
  - `p-5` elements with 0 px padding: 0; console errors: 0; theme matched on all 56 loads.
- **Destructive actions** at 1280 and 390 px in light and dark, 16 measurements:
  - before: "+ Tagihan Baru" and "Catat pembayaran manual" were 4.74:1 (light) and 2.83:1 (dark). "Ya, lanjutkan" was grey text at 1.42:1 (light) and 1.01:1 (dark). "Ya, Buat Kunci Baru" was 4.74 and 8.02:1;
  - after: all four were white on `#E02038` (4.74:1, hover `#9F1239` 8.02:1) in light, and on `#9F1239` (8.02:1, hover `#881337` 9.57:1) in dark.
- **Primary buttons:** 22 measurements, unchanged from the previous change. The lowest was 5.39:1 at rest and 4.73:1 on hover.
- **Beranda card** at 1280 and 390 px in light and dark:
  - before (screenshot from the previous change): "Uang keluar sejauh ini Rp 0";
  - after: "Uang keluar sejauh ini Rp 3.600.000". `current_to_date.outflow` 3.600.000 equals the overview `total_outflow` 3.600.000. The compared `current.outflow` stays 0, because the window was shortened to 1–30 October.
- **Lime in use** (phone Beranda): the active bottom-nav pill is 8.83:1 in both themes. "Kelola →" and "Lihat Semua →" are 9.97:1 in dark and 1.71:1 in light (see design.md, Open decisions).
- **OpenSpec:** `git diff --check` exit 0. `openspec validate fix-web-tokens-and-spending-so-far --strict` and `openspec validate --all` exit 0.

## Not verified

- The internal-movement "Ya, hapus" was not opened in the browser. It uses the same token classes as the measured buttons.
- `Badge`, `MetricTile`, `ProgressBar` and `Card` render on no page. Their token pairs are covered only by `tokens.test.ts`.
- Widths between 640 and 900 px; keyboard, axe and screen-reader runs.
