# Verification — 2026-10-31

Branch `fix/phone-spacing-tokens` from origin/main e1bde53. All checks ran locally on synthetic data. No production host, phone or real AI was used. This is not a WCAG conformance audit.

## Regression test before the fix

`src/app/tokens.test.ts` on the unchanged tokens: 3 of 3 failed. Undefined properties `--space-md` and `--space-sm` beyond the three known `--color-accent*` gaps; primary text 3.60:1 on `--primary` in `:root` and 3.34:1 in `.dark`. After the fix: 3 of 3 passed.

## Final evidence (working tree of the fix commit)

- **Frontend:** `npm ci` exit 0. `npm test`: 55 files, 280 tests passed: the 3 token tests plus 277 existing ones (the archived organize-settings-page record reports 277 at the base; the base suite was not re-run separately). `npm run lint`: exit 0 with the one existing warning (`review/page.tsx:111` `_done`). `npm run type-check`: exit 0. `npm run build`: exit 0.
- **Backend:** not changed, so the backend suite was not run. The local API served the browser check only.
- **Browser** (headless Brave over CDP on ports 9371 and 9372; uvicorn on 127.0.0.1:8071 with AI disabled; `next dev` on 127.0.0.1:3071; a fresh `postgres:16` migrated by `flyway/flyway:10` to V38; one synthetic owner with one bank account, six transactions, two goals including an emergency fund, and one obligation):
  - Seven pages (Beranda, Transaksi, Analisis, Tinjau, Rekening & Saldo, Target & Tagihan, Settings) at 1440, 1280, 390 and 320 px in light and dark: 56 page loads before and 56 after, full-page screenshots of each.
  - `p-5` elements at 390 and 320 px: 60 of 60 had 0 px padding before; 0 of 60 after, all 16 px.
  - Desktop (1440 and 1280 px): `p-5` paddings, main padding and document width identical before and after on all 28 loads.
  - Horizontal overflow: 0 of 56 before and after. Elements outside the viewport and not in a scroll container: 4 before (the Dana Darurat goal card's action group at 320 px, light and dark), 6 in the first after run (the archive button pushed fully outside the card), 0 after design decision 9.
  - The rendered theme matched the requested theme on all 56 loads. Console errors: 0.
  - Coral elements at 1280 and 390 px in light and dark (22 measurements: "Simpan profil", "Simpan anggaran", "Hubungkan ponsel", "+ Catat", active sidebar "Beranda"): before, white text at 3.60:1 (light) and 3.34:1 (dark), hover 4.74:1 (light) and 2.83:1 (dark), shortcut badge white/90 on black/20. After, text and icons `rgb(10, 13, 20)` at 5.39:1 (light) and 5.82:1 (dark); hover on `#F02A42` 4.73:1 (light) and `#FF6675` 6.86:1 (dark); badge ink on white/30. Font size and weight unchanged in the cropped screenshots.
- **OpenSpec:** `git diff --check` exit 0; `openspec validate fix-phone-spacing-and-primary-contrast --strict` and `openspec validate --all` exit 0.

## Not verified

- The Quick Capture and internal-movement success state (shown for about a second after a save) was not captured in the browser; its colours come from the same tokens the test checks.
- Widths between 640 and 900 px, where `sm:p-6` cards now get 16 px, were not screenshotted.
- No keyboard, screen-reader or axe run in this change; markup, focus styles and semantics are unchanged.
