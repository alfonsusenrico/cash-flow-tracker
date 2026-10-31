# Verification — 2026-10-31

Branch `fix/light-theme-text-contrast` from origin/main d3ed9f1. All checks ran locally on synthetic data. No production host, phone or real AI was used. This is not a WCAG conformance audit.

## Tests that fail before the fix

`src/app/tokens.test.ts` with the new assertions on the previous `tokens.css` and `tailwind.config.js`: 9 of 21 failed (the four text tokens in both themes were undefined, and the Tailwind mapping test failed). After the fix: 21 of 21 passed.

## Final evidence

- **Frontend:** `npm ci` exit 0. `npm test`: 56 files, 310 tests passed. `npm run lint`: exit 0 with the existing `review/page.tsx:111` `_done` warning. `npm run type-check`: exit 0. `npm run build`: exit 0; the built CSS has `.text-rose-400,.text-rose-500,.text-rose-600,.text-rose-700{color:var(--expense-text)}`, `.text-expense{color:var(--expense-text)}`, `.text-primary{color:var(--primary-text)}`, `.bg-rose-500{background-color:var(--color-expense)}` and `.bg-primary{background-color:var(--primary)}`.
- **Backend:** not changed, so the backend suite was not run. The local API served the browser check only.
- **Browser setup:** headless Brave over CDP on ports 9381 and 9382. uvicorn ran on 127.0.0.1:8081 with AI and background jobs disabled, on a fresh `postgres:16` migrated by `flyway/flyway:10` to V39. `next dev` ran on 127.0.0.1:3081. The seed was one synthetic owner with a bank account, a wallet, one income, five expenses, one internal movement, two goals and one obligation.
- **Pages and states:** Beranda, Transaksi, Analisis, Tinjau, Rekening & Saldo, Target & Tagihan and Settings at 1280 and 390 px in light and dark: 28 loads before and 28 after. Every element with its own text, and every icon, whose computed colour is in the lime (hue 85–135°) or coral (hue 340–12°) family was measured against its composited background. The minimum is 4.5:1 for text, 3:1 for icons and large text.

| Theme | Family | Before: elements, below minimum, lowest | After: elements, below minimum, lowest |
| --- | --- | --- | --- |
| Light | Lime | 11, 2, 1.71:1 ("Kelola →" on `#E8ECEF`) | 11, 0, 5.25:1 ("Kelola →", `#2D6E22` on `#E8ECEF`) |
| Light | Coral | 77, 50, 2.68:1 ("(38.9%)" on `#D8DFE5`) | 77, 0, 4.91:1 ("(38.9%)", `#B8142D` on `#D8DFE5`) |
| Dark | Lime | 11, 0, 8.07:1 ("+" icon on `#1E201E`) | 11, 0, 8.07:1 (unchanged) |
| Dark | Coral | 77, 0, 5.00:1 ("(38.9%)" on `#181E2A`) | 77, 0, 5.00:1 (unchanged) |

  - Light coral before: expense amounts at 3.60:1 on white and 3.03:1 on the paper; "Uang Keluar" badge, "Lihat →", "+ Tambah Tagihan", "25% Terbayar", "Puncak: …", the "⚡" debt marker and "Kaitkan pembayaran →" at 3.03 to 3.60:1; the sidebar avatar initials at 2.92:1. After: 6.61:1 on white, 5.57:1 on the paper, 5.36:1 for the avatar on `#E2E8F0`.
  - Lime "+" icons and the "+ Rekening" glyph on the charcoal quick-action buttons: 8.07:1 before and after, in both themes.
  - Horizontal overflow: 0 of 28 loads before and after. Console errors: 0 before and after. The rendered theme matched the requested theme on all 56 loads.
- **OpenSpec:** `git diff --check` exit 0. `openspec validate fix-light-theme-text-contrast --strict` and `openspec validate --all` exit 0.

Screenshots are full-page PNGs named `<page>-<width>-<theme>.png` in the local scratch folders `contrast/before/` and `contrast/after/`; they hold synthetic data only and are not committed.

## Not verified

- Dialogs and transient states (Quick Capture, recurring rules, investment trade and top-up dialogs, form errors, the ledger merge bar) were not opened in the browser. Their coral text uses the same Tailwind utilities, which the build output and the mapping test show resolve to `--expense-text`.
- Hover states were not measured; hover coral uses the same token as rest.
- Widths other than 1280 and 390 px; keyboard, axe and screen-reader runs. Markup and focus styles are unchanged.
