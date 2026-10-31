# Verification — 2026-10-31

Branch `feat/settings-page-layout` from origin/main 35613e4. All checks ran locally on synthetic data. No production host, phone or real AI was used. This is not a WCAG conformance audit.

## Baseline (35613e4, before any change)

- `npm ci`: exit 0. `npm test`: 48 files, 252 tests passed. `npm run lint`: exit 0 with the one existing warning (`review/page.tsx:111` `_done`). `npm run type-check`: exit 0.

## Final evidence (working tree of the feature commit)

- **Frontend:**
  - `npm test`: 54 files and 277 tests passed. The six modal tests were replaced by page-level tests that keep their behavioural assertions (PATCH bodies, invalidations, alias trimming and refusal, mapping removal, key rotation and device order, pairing Escape and focus return), and 31 tests were added or extended: settings layout 3, Profil 4, Mata uang & anggaran 3, Pemetaan notifikasi 5, Perangkat & akses 2, sidebar entry 4, Beranda insight card 6, salary +1, sender names +3.
  - `npm run lint`: exit 0, with the same existing warning. `npm run type-check`: exit 0. `npm run build`: exit 0; `/settings` and its six section routes are prerendered.
  - Per-section saves: the Profil PATCH body is exactly `{ name, name_aliases }` and invalidates only `auth`; the Mata uang & anggaran body is exactly `{ currency, emergency_fund_multiplier, monthly_spending_budget }` (an emptied limit is sent as `null`) and invalidates the money views. A 40-month target and a 2-character alias send nothing.
  - axe (vitest-axe, jsdom): 0 violations for the layout, every section, the confirmation state, the sender search and the insight card states.
- **Backend:** not changed, so the backend suite was not run for this change. `GET /api/dashboard/period-comparison` is used as already specified.
- **Browser** (headless Brave over CDP on port 9431; uvicorn on 127.0.0.1:8631 with AI and background jobs disabled; `next dev` on 127.0.0.1:3631; a fresh `postgres:16` migrated by `flyway/flyway:10` to V38):
  - Seed: one synthetic owner with a parent bank account and two pockets, a wallet, September and October expenses, 14 sender names (one ambiguous, one long name), 10 notification mappings and one paired phone.
  - `/settings` at 1440, 1280, 390 and 320 px in light and dark: all six sections and the phone section list loaded; horizontal overflow 0 on all 56 page loads; the section list marked the open section as current.
  - Keyboard (1280 and 390 px): from each section heading, Tab reached every visible control (Profil 3/3, Gaji & siklus 7/7, Mata uang & anggaran 4/4, Perangkat & akses 4/4, Pemetaan notifikasi 10/10, Nama pengirim 29/29), each with `:focus-visible` and a visible outline or ring. On the phone list, Enter on "Nama pengirim" opened `/settings/pengirim` and focus landed on its heading.
  - Confirmation: "Hapus" moved focus to "Batal"; Escape restored focus to "Hapus nama pengirim BI**A S***"; nothing was removed.
  - Search: "jago" left 6 of 14 names and announced "6 dari 14 nama".
  - 200% root text size on Mata uang & anggaran at 1280 px: no horizontal overflow.
  - `/settings/tidak-ada` returned HTTP 404.
  - Measured text contrast (composited on each element's own surface): all text passed 4.5:1 except white text on the coral primary button, 3.60:1 light and 3.34:1 dark (see design.md, Open decisions for owner). An earlier run found "Ya, hapus" at 2.83:1 in dark because `rose-700` maps to light coral in dark mode; it was fixed (`confirmDangerClass`) and re-measured at 6.6:1 or better in dark and 4.74:1 in light.
  - axe-core 4 in the real browser (WCAG 2 A/AA, 2.1 AA, 2.2 AA) on the main region of every section, with disclosures open, plus the phone list and the confirmation state, at 1280 and 390 px in both themes (28 runs): the only violation is the same primary-button contrast (one node on Profil, Mata uang & anggaran and Perangkat & akses). Gaji & siklus, Pemetaan notifikasi, Nama pengirim, the phone list and the confirmation state had 0 violations.
  - Console errors: 0, apart from the deliberate 404 probe.
- **Beranda insight card** (same server, 1440, 1280, 390 and 320 px, light and dark, no `%` and no "HEMAT" anywhere in the card):
  - Day 1 of a salary cycle (salaries confirmed on 30 Sep and today): "Baru 1 hari berjalan. Perbandingan muncul setelah 3 hari agar tidak menyesatkan. Uang keluar sejauh ini Rp 0." For the same state the overview narrative still reads "pengeluaran turun 100.0% …" with `outflow_delta_pct` −100.0, which the old card turned into a "HEMAT" badge.
  - Calendar month (chosen on Analisis, then Beranda): "Dalam 30 hari pertama, uang keluar Rp 2.110.000, Rp 260.000 lebih tinggi dari periode lalu pada hari yang sama. Pembanding: September 2026, 1 Sep – 30 Sep", the truncation note, three increases and one decrease with amounts.
  - Contrast minimum 4.74:1 light and 6.6:1 dark; overflow 0; console errors 0.
- **`git diff --check`:** clean (`git diff --cached --check` on the commit).
- **OpenSpec:** `openspec validate organize-settings-page --strict` exit 0; `openspec validate --all` exit 0 (63 passed).

Screenshots and logs are in the scratch area and are not tracked.

## Not verified

- Assistive technology (VoiceOver, TalkBack) was not used.
- 400% browser zoom was approximated by the 320 px width; 200% text was checked on one section only.
- No real phone or production host was used; the pairing QR flow was exercised only in unit tests.
