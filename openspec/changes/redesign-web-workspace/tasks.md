# Tasks

Evidence for every task goes into `verification.md` of this change: commands, counts, skips, screenshots and measured values. Use synthetic data only. The browser matrix means 1440, 1280, 1024, 768 and 390 px, each in light and dark, in a headless browser on a local build.

## 1. Tokens, type and theme

- [x] 1.1 Record the baseline before any change: `npm test`, `npm run type-check`, `npm run lint` and `npm run build` results; first-load JS of `/` from the build output; the backend suite on a disposable migrated PostgreSQL with its skips; axe results and screenshots of every route at 1280 and 390 px in both themes. Evidence: the baseline table in verification.md.
- [x] 1.2 Replace `frontend/tokens.css` with the workspace token set (design D2) in light and dark. Add the marked compatibility-alias block for every legacy property still used. Tune the pillar inks, `--k-none` and the semantic inks so the existing contrast and hue rules hold, without the info blue. Extend `src/app/tokens.test.ts`: every used property is defined; text pairs are at least 4.5:1 and icon or mark pairs at least 3:1 on window, surface and soft fills in both themes; pillars are at least 25° from transfer, investment, income and expense and at least 40° apart; no non-transfer blue; `--shadow-xs` is lighter than `--shadow-sm`; the card shadow is applied. Evidence: the number of new cases that fail on the old tokens and pass on the new ones, and the measured value table.
- [x] 1.3 Add Geist (variable woff2, Latin subset) with its OFL licence, switch `layout.tsx` to Geist and Geist Mono, and remove the Plus Jakarta Sans files. Update `src/app/fonts.test.ts`. Evidence: the tests pass; `npm run build` with network access to Google Fonts blocked succeeds; the built output contains Geist and Geist Mono and no Plus Jakarta Sans; the browser shows only same-origin font requests.
- [x] 1.4 Theme (D3): inline pre-paint script in `<head>`, `theme` storage with `light`, `dark` and `system` (absent means system), `matchMedia` listener for system, `.dark` class plus `data-theme-choice`, and stale `theme_preset` and `theme_palette` removed. Tests: stored `dark`; stored `light` on a dark OS; absent value; storage that throws; OS change while on system. Browser evidence: a screenshot taken at first paint on `/ledger` shows the chosen theme with no flash, for light-on-dark-OS and dark-on-light-OS.
- [x] 1.5 Set `<html lang="id">` on every page including login, and title the app with the current product name (OD1). Evidence: a test of the root layout and the browser's document language on `/` and `/auth/login`.

Expected Result: the app renders in the new palette and Geist in both themes with no theme flash, every token pair passes its contrast and hue rule, and legacy pages still find every property they use.

## 2. Shell and navigation

- [x] 2.1 Build `WorkspaceShell` and `SideNav` from one `NAV_GROUPS` constant: groups Harian, Uang and Laporan (no Tanya yet), lime active link with `aria-current`, the Tinjau badge from `/review/summary` and the Laporan badge from the report-ready rule, both announced in words. Collapse to a 76 px rail above 1180 px persists in `sidebarCollapsed`, and the rail is forced from 761 to 1180 px. Tests: groups and order, active link, badge names, collapse persistence, rail tooltips and names. Evidence: Vitest counts and screenshots at 1440 (open and collapsed) and 1024.
- [x] 2.2 Build `TopBar`: title and subtitle from the route table, a `controls` slot, search (Enter opens `/ledger?q=`; ⌘K/Ctrl+K focuses it), theme menu (Terang, Gelap, Ikuti sistem), notifications (report ready and plans due within two days, with a count in the accessible name), "+ Catat" (opens the existing `QuickCaptureModal`) and the account menu (username, Sembunyikan or Tampilkan saldo, Pengaturan, Keluar). Tests cover each control, Esc returning focus to its button, and logout redirecting to login. Evidence: Vitest counts.
- [x] 2.3 Build `PhoneTabBar` and `MoreSheet` for 760 px and below: Beranda, Tinjau with badge, Catat, Transaksi and Lainnya; the sheet lists the other pages, Bantuan and Keluar, traps focus and returns it; the bar uses the safe-area inset and `scroll-padding-bottom`. Remove `BottomNav`, `MobileDrawer` and the dock tokens. Tests: tabs, `aria-current`, sheet focus; `rg` finds no import of the removed components. Evidence: Vitest counts and a 390 px screenshot in both themes with the tab label contrast measured.
- [x] 2.4 Build `CompanionStatus` from `GET /api/devices`: ready with last contact time, needs attention with reason, or no phone paired with a link to `/settings/perangkat`; text in the full sidebar, icon with name and tooltip in the rail. Tests for the three states and a failed request (status hidden, no error thrown). Evidence: Vitest counts.
- [ ] 2.5 (Partly done 2026-11-02: `/ledger` and `/insights` steppers are in the top-bar controls and share the period; `/laporan` keeps its own report picker until phase 4 — see verification.md 3.) Keep `AppContext` unchanged (`timeframe`, `cycleOffset`, `bal`, `fmtMoney`, `openQuickAdd`, `openMovement`, `reviewAttention`, `hideBalances`), and render the existing period stepper in the `controls` slot of `/ledger`, `/insights` and `/laporan` (D15). Update `AppLayout.test`, `Sidebar.test`, `TopBar.test` and `ReviewNavBadge.test` to the new shell. Evidence: tests pass; in the browser, stepping back on `/ledger` and then opening `/insights` shows the same period.
- [x] 2.6 Route compatibility smoke: `/`, `/review`, `/ledger?type=expense&kakeibo=unclassified`, `/insights`, `/laporan?basis=month&start=<date>`, `/accounts`, `/goals`, every `/settings/*`, `/dashboard` and `/auth/login` at 1280 and 390 px in both themes. Evidence: each page renders inside the shell (login without it), filters are visible, no console errors, no horizontal overflow, and axe has no serious or critical violation that the baseline (1.1) did not have.

Expected Result: every page opens in the new workspace, navigation works by mouse, touch and keyboard at all five widths, and the legacy pages behave as before.

## 3. Shared primitives and keyboard

- [x] 3.1 Build `Card`/`CardHeader`, `KpiStrip`/`Kpi`, `Pill`, `Segmented`, `ChipGroup`, `MoneyBar`, `Skeleton`, `EmptyState` and `ErrorState` (with retry) on the tokens. Container-query layouts are plain CSS. Tests: one `vitest-axe` render per primitive in both themes with no violations, `MoneyBar` exposes its segments as text with the bar hidden, and targets are at least 24 px. Evidence: Vitest counts.
- [x] 3.2 Build `Drawer` (side panel above 1180 px, overlay from 761 to 1180, bottom sheet at 760 and below) and `Toast` (`role=status`, optional "Urungkan" callback, 6.5 s, pauses on hover and focus). Tests: focus trap and return, Esc, background scroll lock on the sheet, toast timing and undo callback. Evidence: Vitest counts.
- [x] 3.3 Build `ExplainableFigure` and `Explanation` (`explainable-figures`): button with the dotted underline and the "kok segini?" accessible name; a non-modal popover anchored and kept in the viewport above 760 px; a bottom sheet at 760 px and below; one open at a time; Esc, close and outside click return focus; lines, total, caveats and link rendered from server data only; the unavailable state with its reason. Tests for each spec scenario. Evidence: Vitest counts.
- [x] 3.4 Build the shortcut registry and `HelpDialog` (D8): ⌘K/Ctrl+K, N, ? and Esc globally; region-scoped page shortcuts; ignored inside inputs, textareas, selects and contenteditable; the "Pintasan satu tombol" switch persisted in `shortcuts`. Move the current global `N` handler into the registry. Tests: typing "n" in search does nothing; with the switch off, N and ? do nothing while ⌘K and Esc work; the setting survives a remount. Evidence: Vitest counts.

Expected Result: phases 2–6 have every building block they need, each one accessible on its own and covered by tests.

## 4. Cycle summary backend

- [x] 4.1 Add the expected next salary date to `backend/app/services/salary_cycles.py` (`salary-cycles` "Expected Next Salary Date"): three latest confirmed receipts of the confirmed source, 26–35 days apart, local days within 3, next date clamped to the month's length, status `estimated` or `overdue`, reasons `too_few_receipts` and `irregular_receipts`, never written anywhere. Tests: regular, too few, irregular, overdue, 31 → 30 and 31 → 28 clamps, timezone at the day boundary in Asia/Jakarta, and report windows unchanged by the estimate. Evidence: pytest counts on disposable PostgreSQL.
- [x] 4.2 Add `backend/app/services/cycle_summary.py` (D9–D12). It reuses `get_timeframe_window`, `get_kakeibo_breakdown`, `load_spending_offsets`, the reportable SQL helpers, the review attention rules, plan occurrences, goal accounts and the net-worth summary. Tests for every `beranda-cycle-summary` scenario: Keluar equals the overview Kakeibo total and includes saving; an internal move does not change Keluar; the waiting split per section and the answer moving buckets only; safe-to-spend available, unknown, overdue and over-spent (per day 0, no negative); the linked plan leaving Terjadwal; a funded goal not counted twice; the 30-day window without an expected date; a reimbursement raising the remaining amount; and the month basis without a cycle. Evidence: pytest counts.
- [x] 4.3 Add `GET /api/dashboard/cycle-summary` in `backend/app/routers/dashboard.py` with session and Bearer authentication. Tests: 401 without credentials; two owners on the same day see only their own rows, plans, goals and accounts; lines add up for every figure; `remaining = masuk − keluar − scheduled`; the statement count is the same with 1 and 40 transactions, 1 and 5 plans, 1 and 4 goals. Evidence: pytest counts.
- [x] 4.4 Run the full backend suite on a disposable migrated PostgreSQL. Evidence: pass, fail and skip counts in verification.md, each skip with its reason.

Expected Result: one owner-scoped endpoint returns every Beranda figure with lines that add up, and safe-to-spend never divides by a guessed date.

## 5. Beranda

- [x] 5.1 Desktop Beranda (`beranda-cycle-summary`, `finance-dashboard-cockpit`, `observed-insights`):
  - lede with exactly three explainable figures, worded per OD7;
  - KPI strip (Aman with the days caption, Keluar and Masuk with signed-amount fair-comparison chips that hide below 3 days, Kekayaan bersih without a chip);
  - Uang siklus ini with `MoneyBar` and the over-spent message;
  - Kerjakan dulu with review items opening `/review` at the item, due plans keeping link, skip, record and unlink through the existing mutations, and the automatic-rows note;
  - Aktivitas terbaru with top-ups lavender and "Top up Investasi", movements sky and once, waiting rows "Nunggu jawaban", the detail opening the existing dialog;
  - Kakeibo with six buckets and ledger links;
  - Ketahanan Dana with the unknown state;
  - the insight using "belanja" wording without percentages.

  Tests for each scenario of those specs, using fixtures that mirror the preview numbers. Evidence: Vitest counts.
- [x] 5.2 Phone Beranda at 760 px and below (`mobile-native-layout`): safe-to-spend card first with the Dipakai/Terjadwal/Aman split and the waiting amount, then Kerjakan dulu, period figures, Kekayaan bersih, Hari ini and the insight. Explanations open as sheets. The tab bar never covers the last card. Tests for the order and for figures identical to desktop with the same data. Evidence: Vitest counts.
- [x] 5.3 States: loading skeletons in the final layout; the error state with "Coba lagi" and no Rp 0 when the summary fails, while other cards still render; safe-to-spend unavailable for each reason; nothing waiting; month basis without a cycle, with the link to Pengaturan > Gaji. Tests for each. Evidence: Vitest counts.
- [x] 5.4 Animate KPI figure changes over about 380 ms ease-out with `useAnimatedCounter`, and show the value at once under reduced motion. Tests for both. Evidence: Vitest counts.
- [x] 5.5 Remove what Beranda no longer uses: `HeroWalletCard`, `MetricMatrixGrid`, `CumulativeCashFlowChart`, `CategoryDonutChart`, `ObservedCashFlowCard`, `MobileHomeView`, `NarrativeInsightCard`, their tests, and `PendingScheduledBanner` and `ReportReadyNotice` if nothing else imports them. Keep `EmergencyCoverage` and `KakeiboBenchmark` for Rekening and Analisis. Evidence: `rg` shows no import of removed files; type-check and build pass.
- [x] 5.6 Update AGENTS.md "Visual and language references" (new token source, `lang` follow-up closed) and the Beranda and navigation parts of `docs/NEW_USER_GUIDE.md`. Evidence: the diff.

Expected Result: Beranda shows the approved design on desktop and phone, its numbers match the server and explain themselves, and nothing it replaced is left behind.

## 6. Integration verification

- [x] 6.1 Run `npm test`, `npm run type-check`, `npm run lint` and `npm run build` in `frontend`, the backend suite on a disposable migrated PostgreSQL, `openspec validate --all --strict` and `git diff --check`. Evidence: pass, fail and skip counts tied to the commit.
- [x] 6.2 Browser matrix on `/` with a synthetic seeded owner. Checks: axe with zero serious or critical violations; measured contrast of figures, pills, badges and tab labels; no horizontal overflow; no console errors; one screenshot per cell; each of the three figures opens a popover above 760 px and a sheet at 390 px; a keyboard-only walk from the skip link through the lede, KPIs, Kerjakan dulu and an explanation and back; 200% and 400% zoom at 1280 px without horizontal scroll; reduced motion. Evidence: the matrix table with counts and 10 screenshots.
- [x] 6.3 Legacy routes matrix at 1280 and 390 px in both themes (as in 2.6) after all groups. Evidence: no new serious or critical axe violation against the baseline, and the period stepper works on the three period pages.
- [x] 6.4 Gates: first-load JS of `/` is not larger than the baseline from 1.1, and `GET /api/dashboard/cycle-summary` answers in under 300 ms locally on the synthetic dataset (median of 20 calls). Evidence: the numbers before and after.
- [x] 6.5 Cross-check with the preview: seed a synthetic owner that mirrors the preview ledger (49 transactions) and compare Beranda figures with the preview's. Every difference must be explained by D9 (waiting rows stay counted) or D10–D11 (expected date and goal rules). Evidence: the comparison table.

Expected Result: phase 1 is proven locally at all five widths in both themes, with no regression on the pages it did not redesign.

## 7. Acceptance

- [ ] 7.1 Present verification.md, the screenshots and the open decisions (OD1–OD8) to the owner. Record the acceptance and its date in this change and in PROJECT_STATE.md. Evidence: the owner's words quoted with the date.

## Workflow follow-up

- After acceptance: `openspec archive redesign-web-workspace` on the branch, then `openspec validate --all --strict` exits 0.
- Push `feat/web-workspace-redesign` and report the compare link. The owner opens and merges the pull request, and the release runs only through `deploy.yml`. Production confirmation is tracked in PROJECT_STATE.md.
- Start phase 2 (`redesign-review-triage`) from the merged `main`.
