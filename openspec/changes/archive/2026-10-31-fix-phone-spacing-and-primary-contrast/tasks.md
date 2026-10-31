# Tasks

## 1. Regression test and fixes

- [x] 1.1 Add `frontend/src/app/tokens.test.ts`: undefined custom properties in the stylesheets, and primary text contrast at rest and on hover in both themes. Evidence: the test fails on the unchanged tokens (`--space-sm`, `--space-md` undefined; 3.60, 3.34, 4.74 → white fails) before the fix.
- [x] 1.2 Define `--space-sm` and `--space-md` in `tokens.css`. Evidence: the undefined-property test passes.
- [x] 1.3 Set `--primary-contrast` to `#0A0D14` in both themes and the light `--primary-hover` to `#F02A42`; use `--primary-contrast` in `.btn-primary`, the `Button` primary variant, the "+ Catat" badge and the Quick Capture and internal-movement success state. Evidence: the contrast test passes; `rg "text-white" frontend/src/components/ui/Button.tsx` shows only the danger variant.

- [x] 1.4 Keep the goal card actions inside the card at 320 px: `min-w-0` on the title group, `shrink-0` on the action group in `app/goals/page.tsx`. Evidence: the browser check reports no newly off-screen element on Target & Tagihan at 320 px compared with the before run.

Expected Result: the tokens the stylesheet uses all exist, and primary text measures at least 4.5:1 in every state and theme.

## 2. Verification and delivery

- [x] 2.1 Frontend `npm ci`, `npm test`, `npm run lint`, `npm run type-check`, `npm run build` on the final tree. Evidence: exit codes and counts in verification.md.
- [x] 2.2 Headless Brave over CDP against a local backend on a disposable PostgreSQL migrated V1→latest with synthetic data: Beranda, Transaksi, Analisis, Tinjau, Rekening & Saldo, Target & Tagihan and Settings at 1440, 1280, 390 and 320 px in light and dark, before and after. Evidence: zero `p-5` elements with 0 px padding at phone widths after the fix, no new horizontal overflow or off-screen elements, zero console errors, unchanged desktop measurements, screenshot paths in verification.md.
- [x] 2.3 Coral button check before and after at 1280 and 390 px in light and dark: computed text colour and contrast at rest and on hover for "Simpan profil", "Simpan anggaran", "Hubungkan ponsel", "+ Catat" and the active sidebar item, with cropped screenshots. Evidence: every measured ratio at least 4.5:1 after the fix.
- [x] 2.4 `git diff --check`, `openspec validate fix-phone-spacing-and-primary-contrast --strict`, `openspec validate --all`; final diff review. Evidence: exit codes in verification.md.
- [x] 2.5 Archive the change, re-validate, commit, push `fix/phone-spacing-tokens` and report the compare link. Evidence: pushed branch.

Expected Result: both defects are fixed with browser evidence and the branch is pushed for the owner's review.
