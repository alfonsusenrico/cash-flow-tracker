# Verification — redesign-web-workspace, phase 1

All checks ran locally on 2026-11-02 against synthetic data only: a disposable PostgreSQL 16 container migrated by Flyway (schema v43), a loopback API with AI, background jobs and outbound HTTP disabled, and a production `next build` served by `next start` behind a loopback proxy that stands in for nginx (`/api/*` → API). No production system was contacted. Evidence files and screenshots live in the session scratchpad (`web-p1/`), outside the repository.

Revisions: baseline `6a0f255`; backend `258da22`; frontend `7ec89e7` and `8d1f887` (final working tree for the frontend checks, docs and fixes included).

## 1. Baseline (task 1.1, revision 6a0f255)

| Check | Result |
| --- | --- |
| `npm test` | 62 files, 419 tests passed |
| `npm run type-check` | passed |
| `npm run lint` | 0 errors, 1 warning (`review/page.tsx` `_done` unused, pre-existing) |
| `npm run build` | passed |
| First-load JS of `/` | 18 scripts, 1191.3 KB raw, 341.4 KB gzip (sum of the scripts the prerendered HTML loads) |
| Backend suite (Flyway v43, `REQUIRE_TEST_DATABASE=1`) | 1550 passed, 0 failed, 0 skipped, 1 warning (passlib `crypt` deprecation) |
| Browser, every route at 1280 and 390 px, light and dark (60 cells) | Only serious/critical axe finding: `/accounts` color-contrast, 2 nodes (`.opacity-75 text-[10px]` captions), at 1280 light and dark. Overflow 0 px everywhere. Console: the login page logs the expected 401 of `/api/auth/me`. `lang="en"` everywhere; the login page ignored the dark theme |

The baseline browser pass used the backend extracted read-only from `6a0f255`, so in-progress backend edits did not leak into it.

## 2. Tokens, type and theme (tasks 1.2–1.5)

- **1.2 tokens.** `tokens.test.ts` has 99 cases; run against the old `tokens.css` 53 of them fail, against the new one all 99 pass. New cases: text pairs on window, surface and soft fills in both themes; marks (focus, status dot) at 3:1; pillars ≥ 25° from income, expense, transfer and investment fills and inks and ≥ 40° apart; no non-transfer blue and no `--info*` token; `--shadow-xs` lighter than `--shadow-sm`; card and floating shadows applied; legacy names only inside the marked alias block.

  | Pair | Light | Dark |
  |---|---|---|
  | --text on --window | 15.68 | 14.84 |
  | --muted on --surface-3 | 4.73 | 5.04 |
  | --muted on --surface (tab labels) | 5.80 | 6.35 |
  | --lime-ink on --lime | 8.01 | 9.07 |
  | --lime-text on --lime-soft | 5.39 | 8.24 |
  | --ink-fg on --ink | 14.03 | 14.57 |
  | --in-fg on --in-bg | 5.00 | 7.95 |
  | --out-fg on --out-bg | 5.31 | 8.56 |
  | --tr-fg on --tr-bg | 5.82 | 8.29 |
  | --inv-fg on --inv-bg | 5.87 | 8.53 |
  | --warn-fg on --warn-bg | 5.72 | 7.96 |
  | --ok-fg on --ok-bg | 6.07 | 8.87 |
  | --danger-contrast on --danger-solid | 6.05 | 7.45 |
  | pillars on --surface-2 (need, want, culture, unexpected, saving) | 5.07–5.13 | 6.72–6.75 |
  | --k-none on --surface-2 | 5.51 | 5.65 |

  Hues (light/dark): need 178/178, want 330/330, culture 78/78, unexpected 36/36, saving 122/122; income 149–151, expense 10, transfer 207–209, investment 256–259. The preview's info blue is not adopted; focus uses the text colour.
- **1.3 fonts.** `fonts.test.ts` 5 passed. `npm run build` with `HTTPS_PROXY`/`HTTP_PROXY` pointed at a dead port passed. `.next/static/media` holds exactly two woff2 files, byte-identical to `Geist-latin-variable.woff2` and `GeistMono-latin-variable.woff2`; no build file mentions Plus Jakarta. The Geist file is a variable font (`fvar`/`gvar` tables), OFL-1.1 licence shipped. In Brave the only font requests on `/`, `/ledger` and `/auth/login` were the two same-origin `/_next/static/media/*.woff2`; `body` renders in Geist.
- **1.4 theme.** `theme.test.ts` 8 passed (stored dark, stored light on a dark OS, absent value, throwing storage, preset keys removed, OS change while on system). First paint of `/ledger` (captured on the `firstPaint` lifecycle event): stored light on a dark OS → no `dark` class, canvas `rgb(228, 226, 220)`; stored dark on a light OS → `dark` class, canvas `rgb(17, 18, 16)` (`flash-ledger-*.png`).
- **1.5 language and title.** Root layout sets `lang="id"` and the title "CashFlow"; the browser reported `lang="id"` on all 60 legacy-route cells, `/` and `/auth/login`.

## 3. Shell, primitives and keyboard (tasks 2.1–2.6, 3.1–3.4)

Vitest (new or rewritten files): `workspace.test.tsx` 33, `primitives.test.tsx` 26 (vitest-axe per primitive in both themes), `overlays.test.tsx` 13, `explain.test.tsx` 12, `shortcuts.test.tsx` 11, `companion.test.tsx` 11, `AppLayout.test.tsx` 4 — all passed. The old `Sidebar.test`, `TopBar.test` and `ReviewNavBadge.test` were removed with their components; their behaviour is covered by `workspace.test.tsx` and `AppLayout.test.tsx`. `rg` finds no import of `BottomNav`, `MobileDrawer`, `Sidebar` or `layout/TopBar`.

Browser (1440 light unless noted):
- Sidebar 248 px at 1440/1280, 76 px rail at 1024/768, hidden with the tab bar shown at 390. Collapsed at 1440: 76 px, every link keeps a name ("Tinjau, 4 perlu ditinjau", "Laporan, laporan baru siap").
- ⌘K focused "Cari transaksi"; typing "n" there opened nothing; N outside a field opened "Catat transaksi"; ? opened the help; the "Pintasan satu tombol" switch stored `shortcuts=off`, after which N and ? did nothing while Ctrl+K still focused search.
- Theme menu: Terang, Gelap, Ikuti sistem; Esc returned focus to "Tema: Terang". Bell named "Notifikasi, 2 baru" and listed the fresh report and "Streamio Rp 65.000 besok". Account menu: username, Sembunyikan saldo, Pengaturan, Keluar. Search "warung" + Enter opened `/ledger?q=warung` with the text in the ledger search.
- Tab-label contrast at 390: 5.80 (light), 6.35 (dark).
- **2.5 period stepper:** `/ledger` and `/insights` render their stepper in the top-bar controls. Stepping back on `/ledger` (month) and then opening `/insights` showed "Oktober 2026", the same period. `/laporan` keeps its own report picker (design.md, phase 1 placement notes), so 2.5 is not complete for Laporan.
- **Finding (pre-existing, not changed):** in this local production setup `router.replace` on `/ledger` does not update the URL (period chips, type chips, the stepper, and even `window.next.router.replace`), while the same call on `/review` works. The baseline build of `6a0f255` behaves the same, so it is not a regression of this change; it needs its own investigation.
- **2.6 legacy routes** (same 60-cell pass as the baseline, after the change): identical results to the baseline — the only serious axe finding is the same `/accounts` color-contrast pair of nodes at 1280; overflow 0 everywhere; no console errors except the login 401; `lang="id"` everywhere; every page rendered its seeded data inside the shell (`after-legacy/summary.md`, 60 screenshots).

## 4. Cycle summary backend (tasks 4.1–4.4)

- `test_expected_salary_date.py` 14 passed (regular, last three only, too few, three irregular cases, overdue without moving forward, 31→30 and 31→28 clamps, month-end wrap, Asia/Jakarta midnight boundary, receipts after `as_of`, report windows unchanged).
- `test_cycle_summary.py` 21 passed (every 4.2 and 4.3 scenario: Keluar equals the overview Kakeibo total and includes saving, internal move, waiting split per section equal to the badge, answering moves buckets only, safe-to-spend available/unknown/overdue/over-spent with `per_day` 0 and `over_by`, linked plan leaving Terjadwal, funded goal 600k/400k/200k, 30-day window, reimbursement +200k, month basis, no salary source, comparison too_short/no_previous/available, 401 anonymous and unknown Bearer, Bearer equals session, two owners isolated, lines add up, `remaining = masuk − keluar − scheduled`, statement count equal for 1 vs 40 transactions, 1 vs 5 plans and 1 vs 4 goals — 37 statements per summary).
- **4.4 full suite** on a freshly migrated database at `7ec89e7`: **1585 passed, 0 failed, 0 skipped**, 1 warning (passlib `crypt`).

## 5. Beranda (tasks 5.1–5.6)

- `page.test.tsx` 24 passed: exactly three explainable amounts in the summary and no other amount; Keluar explanation (six buckets, saving note, pillar links); safe-to-spend explanation line by line ending with the figure and the salary-date basis; waiting explanation (in or not in Keluar); nothing waiting; irregular salary; overdue salary; over-spent; header and month fallback with the Pengaturan › Gaji link; KPI chips as signed amounts, omitted before 3 days; Uang siklus ini three labelled amounts with the bar hidden; Kakeibo six buckets and the unclassified link; unknown Ketahanan Dana; insight wording; Kerjakan dulu order and links; settled state with today's automatic rows; recent activity (top-up lavender, movement once in sky, waiting row); the detail dialog; phone figures identical to desktop and order of phone sections; loading placeholders; failure with "Coba lagi" and no Rp 0 while other cards render; vitest-axe with no violations.
- `useAnimatedCounter.test.ts` 2 passed (counts to the exact value; immediate under reduced motion).
- 5.5: `HeroWalletCard`, `MetricMatrixGrid`, `CumulativeCashFlowChart`, `CategoryDonutChart`, `ObservedCashFlowCard`, `MobileHomeView`, `NarrativeInsightCard`, their tests and `ReportReadyNotice` are deleted; `PendingScheduledBanner` stays because "Kerjakan dulu" opens it in a drawer; `EmergencyCoverage` and `KakeiboBenchmark` stay. Type-check and build pass.
- 5.6: AGENTS.md (token source and compatibility block, `lang="id"`) and the Navigation, Beranda and Quick Actions parts of `docs/NEW_USER_GUIDE.md`.

## 6. Integration (tasks 6.1–6.5)

### 6.1 CI recipe (`.github/workflows/checks.yml`)

| Check | Result |
| --- | --- |
| `npm ci` | passed |
| `npm run type-check` | passed |
| `npm run lint` | 0 errors, 1 warning (pre-existing, unchanged) |
| `npm test` | 62 files, **563 tests passed** |
| `npm run build` (dead HTTP(S) proxy) | passed |
| Backend full suite | 1585 passed, 0 failed, 0 skipped |
| `openspec validate --all --strict` | 71 passed, 0 failed, exit 0 |
| `git diff --check` | clean |

### 6.2 Beranda matrix (`web-p1-matrix/`)

Seeded owner `beranda_contoh` (synthetic). Every cell: axe (wcag2a/aa, 2.1 a/aa, 2.2 aa) **0 violations**, overflow 0 px, 0 console errors, 0 interactive targets under 24 px outside running text, one full-page screenshot.

| Width | Light | Dark | Explanations |
| --- | --- | --- | --- |
| 1440 | pass | pass | 3/3 popovers in the viewport, not covering the figure, focus inside, Esc closes and returns focus |
| 1280 | pass | pass | same |
| 1024 (rail) | pass | pass | same |
| 768 (rail) | pass | pass | same |
| 390 (tab bar) | pass | pass | 3/3 bottom sheets, focus inside, page scroll locked, Esc returns focus |

Measured minimum contrast (light/dark): figures 5.80/6.35, pills and chips 5.31/7.96, badges 6.35–9.25/8.08–9.46, tab labels 5.80/6.35, captions 5.32–5.80/6.35, navigation 8.01/9.07, summary text 15.68/14.84.

Keyboard walk from the skip link: skip link → Enter moves focus to `main` → search, theme, notifications, Catat, account → the three summary figures → the four KPI figures → Uang siklus ini figures and "Lihat tagihan" → Aktivitas terbaru rows → insight → Kerjakan dulu actions → Kakeibo figure and pillar links; every stop had a visible outline (the search field shows focus on its container ring).

Zoom at a 1280×900 window: 200% (640 CSS px) and 400% (320 CSS px) — overflow 0, the phone layout applies, the explanation opens as a sheet with no horizontal overflow and focus returns on close. Reduced motion: transitions collapse to 0.01 ms and the KPI shows its final value.

Screenshots: `beranda-{1440,1280,1024,768,390}-{light,dark}.png`, `beranda-1440-light-collapsed.png`, `beranda-1440-light-explain.png`, `beranda-zoom200.png`, `beranda-zoom400.png`, `flash-ledger-*.png`, `insights-stepper-1280-light.png`.

### 6.3 Legacy routes

See 2.6: no serious or critical axe violation that the baseline did not have.

### 6.4 Gates

| Gate | Before | After |
| --- | --- | --- |
| First-load JS of `/` (gzip) | 341.4 KB (18 scripts) | **247.8 KB** (16 scripts); recharts no longer loads on Beranda |
| `GET /api/dashboard/cycle-summary`, synthetic owner, 20 calls after one warm-up | — | median **13.3 ms**, p95 15.2 ms, max 17.8 ms |

### 6.5 Comparison with the preview (same synthetic ledger, 2 November, day 9)

| Figure | Preview | App | Explanation |
| --- | --- | --- | --- |
| Keluar | Rp 3.164.500 | Rp 3.521.500 | +357.000: QRIS (45.000) and Toko Serba Ada (312.000) wait in Tinjau but stay counted as "Belum diklasifikasi" (D9) |
| Masuk | Rp 9.800.000 | Rp 9.800.000 | equal |
| Waiting | 4 · Rp 581.000 | 4 · Rp 581.000 | equal; the app adds that 357.000 of it is already inside Keluar |
| Terjadwal | Rp 1.713.500 | Rp 995.000 | Bills equal (995.000). Goal set-asides differ by D11/OD3: Dana Darurat has no target date (no set-aside); Liburan Lombok's 300.000 set-aside is already funded by 400.000 moved into its pocket this cycle |
| Sisa | Rp 4.922.000 | Rp 5.283.500 | follows from the two lines above |
| Aman dipakai hari ini | Rp 214.000 | Rp 229.000 | 5.283.500 ÷ 23 days (2–24 Nov, expected salary 25 Nov from receipts 25 Agu, 25 Sep, 25 Okt), floored to Rp 1.000 |
| Kekayaan bersih | Rp 46.330.000 | Rp 46.330.000 | equal |
| Keluar vs day 9 of the last cycle | +5% | +Rp 511.500 | amounts, not percentages (OD5); the waiting rows are counted (D9) |
| Belanja vs day 9 | Rp 45.500 lebih hemat | Rp 311.500 lebih banyak | the 357.000 of waiting rows count in the app (D9) |

Visual comparison: the 1440 light and dark screenshots follow the preview's composition (window, grouped sidebar with badges, top bar, summary sentences, KPI strip, Uang siklus ini, Kerjakan dulu, Aktivitas terbaru, Kakeibo, Ketahanan Dana, insight) and its tokens. Documented deviations: product name "CashFlow" with a neutral mark (OD1); no net-worth delta (OD4); signed-amount chips and no "vs kemarin" chip on Aman (OD5); the third sentence "{n} hal nunggu jawabanmu (Rp X)" (OD7); tuned pillar hues and no info blue (D2); the safe figure only from a regular salary date (D10); waiting rows counted (D9).

Review fix after the matrix (3ea44b8): at 1440 the Terjadwal segment of "Uang siklus ini" was narrower than its amount and clipped it ("Rp 995.00"). A segment of 8.5rem or less now shows colour only (container query on each segment); the legend below repeats every amount. Checked in headless Brave with the shipped rules at bar widths 690, 760 and 1000 px and with "Rp 999.999.999" at 136 px: no clipped text. After the fix, frontend type-check, lint (0 errors, 1 pre-existing warning), `npm test` (62 files, 563 passed) and build passed.

### 6.6 Acceptance change: Kerjakan dulu in one column (2026-11-02)

The owner accepted with "accepted, defaults ok, move Kerjakan dulu up". BerandaView now renders "Kerjakan dulu" right after "Uang siklus ini" when the Beranda container is 900 px or narrower (`useWidthAtMost`, the same width as `@container main (max-width: 900px)`).

- Vitest: new test in `src/app/page.test.tsx` ("follows Uang siklus ini when the grid is one column…") fails on the previous BerandaView (1 failed, 24 passed) and passes with the change (25 passed).
- Headless Brave on a fresh Flyway v43 database with the synthetic seed (`seed.py`), production build behind the loopback proxy. DOM order of the desktop cards, axe (WCAG 2.2 AA tags), horizontal overflow:

| Theme | Width | Card order | axe | Overflow |
| --- | --- | --- | --- | --- |
| light, dark | 1440 | Uang siklus ini › Aktivitas terbaru › Kerjakan dulu › Kakeibo › Ketahanan Dana | 0 | 0 |
| light, dark | 1024 | Uang siklus ini › Kerjakan dulu › Aktivitas terbaru › Kakeibo › Ketahanan Dana | 0 | 0 |
| light, dark | 800 | Uang siklus ini › Kerjakan dulu › Aktivitas terbaru › Kakeibo › Ketahanan Dana | 0 | 0 |

Console errors across the six cells: 0. The local stack (container, API, proxy, Next) was removed afterwards.

## 7. Not verified

- Owner acceptance (task 7.1).
- Real phones and screen readers (VoiceOver, TalkBack); the checks used headless Brave, axe and keyboard emulation.
- Safari and Firefox rendering.
- The `/laporan` top-bar stepper (deferred, see 2.5) and the pre-existing `/ledger` `router.replace` finding.
- CI on GitHub runs only after the push.
