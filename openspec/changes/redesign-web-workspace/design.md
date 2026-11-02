# Design

## Context

See proposal.md for why and what. This section records the audit of 2026-11-02 that the plan rests on: the worktree at `origin/main` 6cc6704 and the approved preview `cash-flow-tracker-prototypes/2026-11-02-web-redesign/` (`combined.html`, `src/combined/*`, `screenshots/combined-*.png`). The audit separates what was observed in code from what is assumed.

### Current frontend (observed)

- **Stack.** Next.js 16.2 App Router, React 18.3, TanStack Query 5, Tailwind 3.4 (`darkMode: "class"`), recharts 2.14, Vitest with `vitest-axe`. Every page is a client component. The API goes through `src/lib/api.ts`, and `next.config.mjs` rewrites `/api/*` to the backend in development.
- **Shell.** `src/app/layout.tsx` (Plus Jakarta Sans and Geist Mono via `next/font/local`, `lang="en"`), `components/layout/AppShell.tsx` and `AppLayout.tsx`. `AppLayout` holds the `AppContext` used by every page: `timeframe`, `cycleOffset`, `bal`, `fmtMoney`, `openQuickAdd`, `openMovement`, `reviewAttention`, `theme` and `hideBalances`. It also holds the theme (light or dark only, applied in an effect after the first paint), the global `N` key and `QuickCaptureModal`. Navigation: `Sidebar.tsx` (flat list of 7 links, collapse to 72 px, plus `MobileDrawer`), `TopBar.tsx` (title, global period stepper, hide-balances, theme, "Catat") and `BottomNav.tsx` (capsule dock below 1024 px).
- **Tokens.** `frontend/tokens.css` ("Coral Terminal") defines a coral primary, cool grey paper, the semantic inks and the Kakeibo pillar tokens under `:root` and `.dark`. `tailwind.config.js` maps the legacy palette utilities (`emerald`, `rose`, `sky`, `amber`, `violet`) to those inks. `src/app/tokens.test.ts` (about 45 cases) checks contrast, hue separation and source rules.
- **Routes.** `/` (Beranda, `app/page.tsx`, 716 lines), `/review` (344), `/ledger` (2136), `/insights` (1007), `/laporan` (206), `/accounts` (2304), `/goals` (1438), `/settings` with `profil`, `gaji`, `anggaran`, `pemetaan`, `pengirim` and `perangkat`, `/auth/login`, and `/dashboard` (redirect).
- **Beranda today.** It reads `GET /api/dashboard/overview?timeframe&cycle_offset` (15 s polling) and `/accounts`. It renders `HeroWalletCard`, `MetricMatrixGrid`, `ObservedCashFlowCard`, `NarrativeInsightCard` (`/dashboard/period-comparison`), `CategoryDonutChart`, `CumulativeCashFlowChart`, `KakeiboBenchmark`, `PendingScheduledBanner` (`/recurring/pending` with link, skip and record), `ReportReadyNotice` (`/reports/monthly/latest`) and, below 1024 px, `MobileHomeView`.

### Backend facts that constrain the design (observed)

- `GET /pulse` and `GET /dashboard/overview` deliberately return `safe_to_spend_today: null` ("a future salary date is unknown"). Salary cycles start only at confirmed receipts, and the open cycle has no known end (`services/salary_cycles.py`, `salary-cycles` spec). `clean-core-ledger`, `finance-dashboard-cockpit` and `reimbursement-offsets` forbid a daily allowance from a guessed end.
- The overview's `kpis.total_outflow` excludes movements and the "Investasi" category. The Kakeibo breakdown (`get_kakeibo_breakdown` in `routers/dashboard.py`) adds net fresh saving through movements into goal and investment accounts. The owner's decision "savings and top-ups count in Keluar" therefore matches the Kakeibo total, not `total_outflow`.
- Rows that need review stay recorded and counted (AGENTS.md: settled notifications are always recorded, and doubt sets `review_status = needs_review`). Merchant-less rows count as "Belum diklasifikasi". One-sided "Internal Movement" legs are not reportable spending. The preview's idea that items in Tinjau are excluded from totals is therefore false for the real ledger.
- Goals have `target_amount`, `current_amount`, `target_date` and `is_emergency`, but no per-cycle contribution field.
- Existing endpoints cover most preview data. Review: `/review/summary`, `/review/queue`, `/review/bulk` and `/review/topup-suggestions/reject`. Movements: `/movements/complete` and `/movements/merge`. Top-ups: `/investment-topups/from-transaction`. Text capture: `/capture/text`, `/{id}/answer`, `/{id}/undo` and `/suggestions`, which the web does not use yet. Insights: `/dashboard/period-comparison`, `/cash-flow-history`, `/recurring-charges`, `/question-rate` and `/analytics`. Reports: `/reports/monthly*`. Devices: `/devices` with `last_used_at` and `health`. Transactions: `/transactions` with `q`, `kakeibo_type`, `source`, `review_status` and `period` filters and a `summary`.
- CSP (`nginx/default.conf`): `script-src 'self' 'unsafe-inline'` and `font-src 'self'`. An inline pre-paint theme script is allowed, and fonts must be self-hosted.

### Preview facts (observed in `src/combined`)

- Breakpoints: full sidebar above 1180 px, icon rail and overlay drawer from 761 to 1180 px, phone layout with tab bar at 760 px and below. Main-column grids use container queries on `main`.
- Its tokens fail three existing rules (measured with a scratch script): light `--k-none` text is 4.10:1 on white, the saving hue (216°) sits 7–8° from the transfer ink, and unexpected (30°) sits 18–20° from the expense ink. Need (179°) and saving (216°) are 37° apart, and unexpected and culture 34° apart, both below the 40° pillar rule. The info blue (`--info-*`) would be a second blue next to transfer sky.
- Its safe-to-spend formula is `(masuk − keluar − tagihan − setoran target) / days to payday`, floored to Rp 1.000. That needs a next payday, which the real app does not have (see D10).

### Map: preview → current code → data → gap

| Preview page or section | Replace or reuse | Data today | Gap (phase) |
| --- | --- | --- | --- |
| Shell: window, grouped sidebar, badges, top bar, tab bar, Lainnya | Replace `AppLayout` chrome, `Sidebar`, `MobileDrawer`, `TopBar`, `BottomNav`. Keep the `AppContext` API | `/auth/me`, `/review/summary`, `/reports/monthly/latest`, `/devices` | None (P1) |
| Theme light, dark, system | Replace theme handling in `AppLayout` and `TopBar` | Browser storage | None (P1) |
| Search ⌘K | New. Opens `/ledger?q=` (`lib/ledgerFilters.ts` already reads `q`) | `/transactions?q=` | None (P1) |
| Notifications bell | New, replaces `ReportReadyNotice` placement | `/reports/monthly/latest`, `/recurring/pending` | None (P1) |
| Beranda lede, three numbers, "Kok segini?" | Replace `page.tsx`, remove `HeroWalletCard`, `MetricMatrixGrid`, `CumulativeCashFlowChart`, `CategoryDonutChart`, `ObservedCashFlowCard`, `MobileHomeView` | Partly in overview | **`GET /dashboard/cycle-summary`** with explanation lines, waiting split, scheduled, expected salary date, safe-to-spend (P1) |
| KPI strip | New primitive | Cycle summary + `/dashboard/period-comparison` | Net-worth delta needs history (deferred, OD4) |
| Uang siklus ini | New | Cycle summary | (P1) |
| Kerjakan dulu | New. Reuses `PendingScheduledBanner` actions | `/review/queue`, `/recurring/pending` | None (P1) |
| Aktivitas terbaru, Hari ini | Rewrite. Reuses `lib/provenance.ts` and `lib/ledgerMovements.ts` | overview `recent_transactions` | None (P1) |
| Kakeibo card | Rewrite. Reuses `lib/kakeibo.ts` | overview `kakeibo` | None (P1) |
| Ketahanan Dana | Rewrite. Reuses `EmergencyCoverage` logic | overview `kpis` coverage fields | None (P1) |
| Insight banner | Rewrite `NarrativeInsightCard` | `/dashboard/period-comparison` | None (P1) |
| Tinjau triage (list + one item, folders, J/K/E/T/1–9/Z/?, auto-advance, Urungkan) | Replace `review/page.tsx` layout. Reuse `PurposeQuestionCard`, `MovementReviewCards` and `HistorySuggestionsSection` logic and `lib/review.ts` | `/review/queue`, `/review/bulk`, `/movements/*`, `/investment-topups/from-transaction`, `/ingest/notifications/{id}/answer-purpose`, `/review/suggestions*` | **Server-side undo** for each review action (P2). Item order and next item are client-side |
| Transaksi: tabs, chips, search, day groups with totals, drawer incl. Top up Investasi, provenance, "Tandai sudah dicek", delete with undo | Restyle `ledger/page.tsx` and `MobileLedgerFeed`. Reuse `DetailDrawer`, `ExpenseTopupConversionModal`, `ledgerFilters` | `/transactions*`, `/review/bulk accept`, `/investment-topups/from-transaction` | **Per-day totals** across pages. **Undo after delete** (OD6) (P3) |
| Catat dialog (text first, live preview, refuses text without amount) | New dialog. Keep `QuickCaptureModal` as "Isi manual" | `/capture/text`, `/capture/{id}/answer`, `/capture/{id}/undo` | **Parse-only preview** without writing (P3) |
| Analisis Ringkasan + Siklus/Bulan toggle | Restyle `insights/page.tsx`. Reuse `ObservedInsights`, `QuestionRate`, `KakeiboBenchmark`, budget editing | `/dashboard/period-comparison`, `/cash-flow-history`, `/recurring-charges`, `/question-rate`, `/analytics`, `/categories` | None (P4) |
| Analisis Kalender (heat, day sheet, bills timeline, phone week strip) | New | Partial: `/analytics` daily cadence, `/recurring` | **`GET /dashboard/calendar`** per-day aggregate incl. waiting and plan occurrences (P4) |
| Analisis Peta Aliran (Sankey, keyboard nodes, table, drawer, replay, "Masih ada" folded) | New. Port the preview's `sankey.js` | None | **`GET /dashboard/flow`** nodes and links (P4) |
| Laporan | Restyle `laporan/page.tsx` and `components/report/*` | `/reports/monthly*`, `/classification/*` | None (P4) |
| Rekening & Kantong | Restyle `accounts/page.tsx`. Reuse every modal | `/accounts*`, `/dashboard/net-worth`, `/investment-topups*` | None (P5) |
| Target & Tagihan | Restyle `goals/page.tsx`. Reuse `RecurringRulesModal` and `DebtAllocationEditor` | `/goals*`, `/obligations*`, `/recurring*` | None (P5) |
| Pengaturan (HP, Kebiasaan, Tampilan, Akun) | Restyle `settings/*`. Add Kebiasaan and Tampilan | `/devices*`, `/ingest/*aliases`, `/auth/settings`, `/salary/*` | **Read-only habits list** (P5) |
| Tanya | New page | `/capture/*` and the summaries above | **Intent routing** (P6). Free-form answers need a separate LLM change |

Assumptions (not verified in phase 1 planning): the owner's devices report health often enough for a "terakhir nangkep" time; the per-day aggregate and flow queries fit the existing indexes on `transactions(user_id, date)`; `/review/queue` row counts stay within its row limit for one-item triage.

## Goals / Non-Goals

**Goals:**

- One shell and one set of primitives that phases 2–6 reuse, so later pages are composition work and not new design.
- Beranda money figures computed once on the server, with explanation lines that add up to each figure, so the web and a future phone or Tanya view cannot drift.
- Every phase leaves `main` deployable: legacy pages keep working inside the new shell until their phase replaces them.

**Non-Goals:**

- No change to ledger semantics, review status, movement pairing or what any report counts (D9).
- No server-side rendering migration. Pages stay client components with TanStack Query.
- No new runtime dependency in phase 1. The Sankey in phase 4 is ported from the preview, not added as a library (D21).

## Decisions

### D1. Six phases, legacy pages inside the new shell

Phase 1 replaces the chrome and Beranda only. The new `AppLayout` keeps the `AppContext` contract unchanged, so `/review`, `/ledger`, `/insights`, `/laporan`, `/accounts`, `/goals` and `/settings` render as today inside the new window. Each later phase replaces one group of pages and deletes the components it made unused.

*Rejected:* a big-bang redesign on a long-lived branch (one unreviewable PR, long drift from `main`, no deployable midpoint). A feature flag with two shells (doubles the test matrix and keeps dead code in production).

### D2. Tokens: preview palette, compatibility aliases, tuned pillars

`tokens.css` gets the preview's semantic set: `--canvas`, `--window`, `--surface`, `--surface-2`, `--surface-3`, `--line`, `--line-strong`, `--text`, `--text-2`, `--muted`, `--tile`, `--ink`, `--lime*`, `--in-*`, `--out-*`, `--tr-*`, `--inv-*`, `--warn-*`, `--ok-*`, `--k-*`, `--shadow-window`, `--shadow-card`, `--focus` and the radii. Every legacy name the code still uses (`--bg`, `--surface-raised`, `--border`, `--text-secondary`, `--primary*`, `--color-income*`, `--color-expense*`, `--color-transfer`, `--color-investment*`, `--color-kakeibo-*`, `--*-text`, `--*-contrast`, `--danger*`, `--accent-lime`, `--shadow-xs|sm|md`, `--sidebar*`) becomes an alias of the new tokens in one marked block. `tokens.test.ts` keeps checking that every used property is defined. The aliases and the legacy Tailwind palette remaps are removed in phase 5, when no legacy page is left.

Pillar inks are tuned to the existing rules, which stay in force: the saving hue moves away from the transfer ink, unexpected away from the expense ink, and each pillar at least 40° from the others. Light `--k-none` text darkens to at least 4.5:1, or "Belum diklasifikasi" text uses `--text-2` with the grey only as its dashed marker. The preview's info blue is not adopted; information uses neutral tokens, as `tactile-pulse-ui` requires. The final values are chosen in task 1.2 with the contrast and hue script as evidence.

*Rejected:* copy the preview tokens verbatim (breaks approved contrast and hue rules); keep "Coral Terminal" and restyle only layout (the owner approved the preview's look).

### D3. Theme: three choices, one class, pre-paint script

Storage key `theme` holds `light`, `dark` or `system` (absent means `system`, so existing values keep working). An inline script in `<head>` resolves the choice before paint and sets `class="dark"` and `data-theme-choice`. While "system" is selected, a `matchMedia` listener follows OS changes. Tailwind keeps `darkMode: "class"`, so legacy `dark:` utilities keep working. All storage access is wrapped in try/catch.

*Rejected:* a media-query-only theme (cannot force light on a dark OS, and breaks legacy `dark:` classes); setting the theme in `useEffect` as today (flashes the wrong theme).

### D4. Fonts

Geist (variable, Latin subset, OFL-1.1 with licence file) replaces Plus Jakarta Sans via `next/font/local`. Geist Mono stays. Body 14 px desktop and 15 px phone, as in the preview, with `tabular-nums` on `body`.

### D5. Breakpoints and layout

Viewport breakpoints for the chrome: phone ≤760, rail 761–1180, full ≥1181 (Tailwind screens `rail: 761px` and `wide: 1181px` added, legacy `sm/md/lg/xl` untouched). Card grids inside the main column use CSS container queries on the `main` container, written in plain CSS (`globals.css` or a component stylesheet). No container-queries plugin is added. The right-hand drawer is a third grid column at ≥1181, an overlay from 761 to 1180, and a bottom sheet at ≤760. Verification widths: 1440 and 1280 (full), 1024 and 768 (rail), 390 (phone).

*Rejected:* keeping the 1024 px phone/desktop split (the preview gives tablets the desktop layout with a rail).

### D6. Shell composition

`components/workspace/`: `WorkspaceShell` (window grid, skip link, `main#main` with `tabIndex=-1`), `SideNav` (groups from one `NAV_GROUPS` constant shared with the tab bar and the Lainnya sheet), `TopBar` (title, subtitle and a `controls` slot used by legacy pages for the period stepper), `PhoneTabBar`, `MoreSheet`, `AccountMenu`, `NotificationsMenu` and `CompanionStatus`. Page titles and subtitles come from a route table, and a page can override them through a small context. "Tanya" stays out of `NAV_GROUPS` until phase 6.

### D7. Shared primitives (phase 1, reused later)

`Card`/`CardHeader` (tile, title, caption, action), `KpiStrip`/`Kpi`, `Pill` (semantic variants), `Segmented`, `ChipGroup`, `MoneyBar` (labelled segments; the bar itself is `aria-hidden`), `ExplainableFigure` + `Explanation` (popover above 760 px, sheet below; renders server lines only), `Drawer` (side or sheet with focus trap and return), `Toast` (`role=status`, optional "Urungkan" with an injected undo callback, 6.5 s, pauses on hover or focus), `ShortcutProvider` + `HelpDialog`, `EmptyState`, `ErrorState` (with retry) and `Skeleton`. Dialogs use the native `<dialog>` element. The existing `Modal`, `DetailDrawer` and `FormField` keep serving legacy pages until their phase.

### D8. Keyboard and WCAG 2.1.4

One registry owns the shortcuts. Global: ⌘K/Ctrl+K, N, ?, Esc. Page scopes, such as Tinjau's J/K/Enter/E/T/1–9/Z in phase 2, register while mounted and fire only when focus is inside their region. Single-key shortcuts are skipped inside inputs, textareas, selects and contenteditable, and can be switched off in the help dialog (storage key `shortcuts`). This makes them conform to 2.1.4 Character Key Shortcuts. The existing global `N` today does not have an off switch.

### D9. Money semantics on Beranda

- **Keluar** = Kakeibo `total_allocated` for the Beranda window: need, want, culture, unexpected, saving (expenses plus net fresh saving transfers) and unclassified, net of reimbursement offsets per pillar. This is the owner's "savings and top-ups count in Keluar". It equals the Analisis Kakeibo total.
- **Masuk** = overview `total_inflow` (reportable income, reimbursements excluded).
- **Waiting** = rows counted by the Tinjau attention badge in the window. They are not removed from any total. Each line says whether it is in Keluar and under which bucket. Answering moves money between buckets, never in or out of Keluar, except a one-sided movement leg that the answer makes reportable.
- The insight banner keeps the fair comparison's outflow and calls it "belanja", because it excludes saving movements (`observed-insights`).

*Rejected:* excluding waiting rows from totals as the preview does (changes every report and contradicts "recorded means committed"). Recording them as "need" until answered (forbidden: a missing pillar is never read as need).

### D10. Safe-to-spend and the expected salary date

`salary-cycles` gains an estimate only: three consecutive confirmed receipts, 26–35 days apart, with days of the month within 3 of each other. The next date is the latest receipt's day in the following month, clamped to the month's length. It is never a boundary. `beranda-cycle-summary` computes:

```
remaining = masuk − keluar − scheduled
days_left = expected_date − today            (today counts; the payday does not)
per_day   = floor(max(0, remaining) / days_left / 1000) × 1000   when status = available
```

Status `unavailable` reasons: `no_cycle` (month basis), `no_expected_salary` (`too_few_receipts` or `irregular_receipts`) and `salary_overdue` (`days_left ≤ 0`). `remaining` is always returned with its lines, so the owner still sees "Sisa" without a daily division. `/pulse` and the overview keep `safe_to_spend_today: null`; the cycle summary is the only producer.

*Rejected:* the deprecated `payday_day` (a guessed end, explicitly retired); dividing by days to month end (invents a boundary); hiding the figure entirely (the owner approved it on the preview).

### D11. Scheduled amount without double counting

- **Plans:** unpaid occurrences (not linked, not skipped, not recorded) of active non-income plans due or overdue from the window start up to the day before the expected date. With no expected date, the window is the next 30 days, as in "Upcoming commitments". A linked payment leaves "Terjadwal" in the same moment it enters "Keluar".
- **Goal set-asides:** only active, non-archived goals with a future `target_date` and a remaining amount. `set_aside = ceil(remaining_at_window_start / months_left)` with `months_left ≥ 1`. `funded` = this window's contributions to the goal (inbound legs into its `goal_accounts` plus goal-linked transactions). The unfunded part is `max(0, set_aside − funded)`. Goals without a target date contribute nothing. Each goal is one explanation line showing set-aside and funded.

### D12. `GET /api/dashboard/cycle-summary` (phase 1)

Session or Bearer. Owner-scoped queries only. Integers in IDR. The response:

```json
{
  "ok": true,
  "as_of": "2026-11-02T16:20:00+07:00",
  "period": {"basis": "cycle", "status": "open", "start_date": "2026-10-25", "today": "2026-11-02",
             "day_index": 9, "label": "Siklus sejak 25 Okt"},
  "expected_salary": {"status": "estimated", "date": "2026-11-25", "basis": "regular_receipts",
                      "receipt_dates": ["2026-08-25", "2026-09-25", "2026-10-25"], "reason": null},
  "keluar": {"total": 3164500, "lines": [{"key": "need", "label": "Kebutuhan", "amount": 1420000,
             "href": "/ledger?..."}]},
  "masuk": {"total": 9800000, "lines": []},
  "waiting": {"count": 4, "total": 581000, "in_keluar": 469000, "not_in_keluar": 112000,
              "lines": [{"section": "questions", "transaction_id": "…", "label": "QRIS 0812-xxxx", "amount": 45000,
                         "in_keluar_as": "unclassified"}]},
  "scheduled": {"total": 1713500, "window_end": "2026-11-24", "lines": [{"kind": "plan", "label": "Cicilan Laptop",
                "due_date": "2026-11-05", "amount": 600000}, {"kind": "goal", "label": "Liburan Lombok",
                "set_aside": 600000, "funded": 0, "amount": 600000}]},
  "remaining": {"total": 4922000},
  "safe_to_spend": {"status": "available", "reason": null, "per_day": 214000, "days_left": 23,
                    "rounding": 1000},
  "net_worth": {"total": 46330000, "liquid": 19480000, "investment": 29250000, "debts": 2400000,
                "lines": []}
}
```

Invariants checked by tests: lines add up to their totals; `remaining.total == masuk − keluar − scheduled`; `waiting.in_keluar + waiting.not_in_keluar == waiting.total`; `keluar.total` equals the overview's `kakeibo` total for the same window; a constant number of SQL statements regardless of row counts. Errors: 401 unauthenticated; 500 surfaces as the Beranda error state. No partial figures are fabricated, and a failing sub-computation fails the request. The overview stays as it is for the legacy pages and the phone.

### D13. Data fetching

TanStack Query with the existing `queryKeys` pattern. New keys: `dashboard.cycleSummary` and `devices.list`. Beranda refetches every 30 s and on focus. All capture, edit, review and plan mutations already invalidate `dashboard.all`, and the new key lives under it. Money formatting stays in `lib/utils.ts` (`fmtMoney`, `bal`, which respects "Sembunyikan saldo"). The client does no money arithmetic beyond formatting server values.

### D14. Routing and URLs

All current paths and query parameters stay (spec "Route Compatibility"). New sub-views in later phases use query parameters on the existing path, not new paths: `/insights?tab=kalender|aliran`, `/review?item=<id>&folder=<key>`, `/ledger?tx=<id>`. Pengaturan keeps its sub-routes. Tanya (phase 6) gets `/tanya`.

### D15. Period stepper placement

The global stepper leaves the top bar. Legacy pages that read `timeframe` and `cycleOffset` get it through the top bar's `controls` slot, rendered by a small `PeriodStepper` wrapper around the existing control, so their behaviour is unchanged. Beranda ignores `cycleOffset` and always describes the current period.

### D16. Beranda composition and what moves

Desktop, matching the preview: lede, KPI strip, then two columns. Left: Uang siklus ini, Aktivitas terbaru, insight. Right: Kerjakan dulu, Kakeibo, Ketahanan Dana. Below 1180 px the columns stack in reading order. The phone layout follows `mobile-native-layout`. Moves:

- `PendingScheduledBanner` actions become "Kerjakan dulu" items. They keep the same mutations and confirmations: link, skip, record, and unlink for undo.
- `ReportReadyNotice` becomes a notification and the Laporan badge.
- The row detail on Beranda reuses the current detail dialog until phase 3 replaces it with the drawer.
- `RecurringRulesModal` stays reachable from "Kerjakan dulu" and Target & Tagihan.

### D17. Accessibility

Target WCAG 2.2 AA: landmarks (skip link, `nav` with labels, `main`), `aria-current` on navigation, badges announced in words, 24 px minimum targets (2.5.8), focus visible from `--focus` with 2 px outline, and focus not obscured by the sticky top bar or tab bar (`scroll-padding`) (2.4.11). Dialogs and sheets trap focus and restore it. Explanations are non-modal dialogs. Reduced motion disables count-up and sheet slides. Reflow at 320 CSS px and 200% text zoom. Colour is never the only carrier (pills have text, bars have labels, unclassified has a dashed marker).

Verification per phase: component tests with `vitest-axe`, then headless-browser checks of every changed page at 1440, 1280, 1024, 768 and 390 px in light and dark (axe-core with zero serious or critical violations, measured text contrast, no horizontal overflow, no console errors, screenshots), plus a manual keyboard walk recorded in `verification.md`.

### D18. Performance

No new dependency. Beranda drops recharts from its route bundle (it stays on Analisis). Gate: `next build` first-load JS for `/` is not larger than before the change, and the cycle summary responds in under 300 ms on the synthetic dataset locally. The cycle-summary statement count is constant (tested).

### D19. Product name

The shell shows the current product name ("CashFlow") with a neutral mark. The preview's "Arus" name and dot logo wait for the owner (OD1). Swapping them later is one constant and one SVG.

### D20. Tests

- Backend: pytest against a disposable migrated PostgreSQL (`TEST_DATABASE_URL`). Cases: cycle summary invariants, owner isolation, the expected-date rule (regular, too few, irregular, overdue, month clamp), safe-to-spend states, the scheduled double-count cases, waiting split per section, the reimbursement effect and the statement count. Skips are reported.
- Frontend: Vitest for tokens (contrast and hue script), theme script (storage present, absent or throwing), shortcut registry (fields, off switch), shell navigation and badges, explanation behaviour, and Beranda states (normal, nothing waiting, safe unavailable, error, loading).

### D21. Sankey (phase 4, decided now so phase 1 adds no dependency)

Port the preview's `sankey.js` layout to a typed module with the table alternative as the primary accessible view.

*Rejected:* `d3-sankey` (new dependency and its own accessibility work); recharts Sankey (limited keyboard and labelling control).

## Phased delivery plan

Each phase is its own OpenSpec change and pull request, in this order, each leaving `main` deployable. Sizes are relative to phase 1 = 10.

| Phase | Change | Scope | Backend work | Gate | Unchanged meanwhile | Size |
| --- | --- | --- | --- | --- | --- | --- |
| P1 | `redesign-web-workspace` (this) | Tokens and compatibility aliases, Geist, theme, shell (sidebar, rail, top bar, tab bar, Lainnya, account menu, notifications, companion status, search), primitives, shortcuts with off switch, `lang="id"`, Beranda desktop and phone | `GET /dashboard/cycle-summary`, expected salary date service | pytest (disposable PG) + Vitest + type-check + lint + build; browser matrix 5 widths × 2 themes on `/` and a smoke pass of every legacy route inside the shell; axe 0 serious; bundle gate | Content of `/review`, `/ledger`, `/insights`, `/laporan`, `/accounts`, `/goals` and `/settings` | 10 |
| P2 | `redesign-review-triage` | Tinjau inbox: folders (Apa ini?, Perlu jawaban, Pindah saldo belum lengkap, Saran dari riwayat, folded Dicatat otomatis), list plus one large item, keys J/K/Enter/E/T/1–9/Z/? scoped to the triage region, auto-advance, Urungkan toast, "Semua beres", phone list → item with back | Undo for each review action: each action returns an `undo_token`, and `POST /api/review/undo` reverses it through the owner-edit path within 10 minutes, appending (never updating) `classification_feedback`; top-up and movement reversals use their existing reversal paths. Probably one Flyway migration for an owner-scoped `review_actions` table (before-state, expiry) | Undo per action type incl. movement legs, top-up reversal, debt progress; keyboard tests; browser matrix on `/review`; axe | Other pages | 6 |
| P3 | `redesign-transactions-workspace` | Transaksi: tabs, chips, search, period stepper in the header, day groups with day totals, detail drawer or bottom sheet with edit and type switch incl. Top up Investasi, provenance, "Tandai sudah dicek", delete; Beranda row detail moves to this drawer. Text-first Catat dialog with live preview and "Isi manual" fallback to the existing form | `daily_totals` in `GET /transactions` for the filtered window (all pages, not only the loaded one); `POST /api/capture/parse` (deterministic parser only, writes nothing, no LLM); delete undo per OD6 | Day totals equal the summary; conversion keeps paired legs; capture parse never writes; browser matrix on `/ledger` and the dialog | Analisis, Laporan, Uang pages, Pengaturan | 7 |
| P4 | `redesign-analysis-and-reports` | Analisis tabs Ringkasan, Kalender, Peta Aliran with the Siklus gaji / Bulan kalender toggle; Kalender heat, day sheet, bills timeline, phone week strip; Peta Aliran Sankey with keyboard node navigation, table alternative, drawer, replay, "Masih ada" folded by default; Laporan restyle | `GET /dashboard/calendar` and `GET /dashboard/flow` (contracts below) | Calendar day sums equal ledger; flow conserves per account; table alternative complete; browser matrix on `/insights` (3 tabs) and `/laporan` | Uang pages, Pengaturan | 8 |
| P5 | `redesign-money-and-settings` | Rekening & Kantong, Target & Tagihan, Pengaturan (HP & perangkat, Kebiasaan, Tampilan with theme and shortcuts, Akun incl. gaji, anggaran, pemetaan, pengirim) on the primitives; remove token aliases, legacy palette remaps and unused legacy components | `GET /api/habits` (read-only list of learned merchant habits with counts; forgetting a habit is a later change) | Every modal still works (top-up, trade, valuation, reconcile, goal adjust, obligation payment, plans); token test has no alias left; browser matrix | Tanya absent | 6 |
| P6 | `add-tanya-conversation` | Tanya page and nav entry: Catat in the composer (capture endpoints), deterministic intents rendered as cards (cycle summary, waiting items, upcoming bills, Kakeibo), suggestion chips, unknown text answered with the chips and no guess; conversation kept in memory for the session only | Intent matching in the frontend over existing endpoints; no new endpoint expected | Intent table tests incl. unknown and Indonesian/English variants; no network call to any model; browser matrix on `/tanya` | — | 4 |

A later, separate change, `add-tanya-llm-answers`, would add free-form answers. That is a **new LLM use**: the model would read the owner's financial data to answer open questions, which differs from today's bounded notification interpretation. It needs, before any code: an eval set with expected answers and refusal cases; numbers taken only from deterministic tool results and never generated; a token and cost budget with a per-user limit; a privacy statement covering what leaves the server, the redaction and the retention (the floor forbids logging prompts or answers without a defined retention); provider mocking in tests; and owner approval of the provider and spend.

### Later-phase contracts (draft, finalised in each phase's design)

- **P2 undo:** `POST /api/review/bulk`, `/movements/complete`, `/movements/merge`, `/investment-topups/from-transaction`, answer-purpose and history-suggestion accept each return `{"undo_token": "<opaque>", "expires_at": "…"}`. `POST /api/review/undo {"undo_token"}` → 200 with the restored rows, 404 `undo_not_found`, 409 `undo_conflict` when a row changed since, 410 `undo_expired`. The token is single use and owner-scoped.
- **P3 day totals:** `GET /api/transactions?...&include=daily_totals` returns `daily_totals: [{"date": "2026-11-02", "inflow": 0, "outflow": 109000, "movements": 500000}]` for the whole filtered window, using the same reportable rules as `summary`.
- **P3 capture parse:** `POST /api/capture/parse {"text"}` → `{"amount": 5000|null, "occurred_at": "…"|null, "time_phrase": "jam 2", "account_hint": …, "category_hint": …, "problems": ["missing_amount"]}`. Deterministic only, writes nothing, rate-limited like `/capture/text`.
- **P4 calendar:** `GET /api/dashboard/calendar?timeframe=cycle|month&cycle_offset=` → per local day `{date, spend, income_count, movement_total, topup_count, waiting_count, waiting_amount, plans_due:[{label, amount}]}` plus window and expected salary marker. Spend uses the reportable rules, so the days add up to the period's spending.
- **P4 flow:** `GET /api/dashboard/flow?timeframe&cycle_offset&through=YYYY-MM-DD` → `nodes [{id, kind: source|account|pillar|saving|investment|remaining, label}]`, `links [{from, to, amount}]`. For each account node, opening balance plus inflows equals outflows plus "Masih ada". Movements between non-goal accounts are links between account nodes, and waiting legs are shown as their own "nunggu jawaban" node, so nothing is silently allocated.
- **P5 habits:** `GET /api/habits` → `[{merchant_label, rule_label, confirmations, last_used_at}]`, owner-scoped, from `merchant_memory` and history, read-only.

## Risks / Trade-offs

- [The safe-to-spend figure is unavailable for many users (fewer than three regular receipts)] → Beranda still shows the remaining amount and says what is missing. OD2 offers an owner-entered expected date as a follow-up.
- [Legacy pages look half-migrated inside the new shell between phases] → Compatibility aliases give them the new palette and type. A smoke pass of every legacy route is part of each phase's gate. Phases are kept short.
- [Token aliases hide a legacy value that fails contrast on the new surfaces] → `tokens.test.ts` measures every alias pair on the new window and surfaces, and the browser axe run covers legacy routes at two widths.
- [Two "outflow" numbers (Keluar with savings, the insight's belanja without)] → Distinct names, and the Keluar explanation lists saving as its own line.
- [Scheduled goal set-asides look invented] → Only goals with a target date count. Each line shows the arithmetic, and OD3 lets the owner drop goals from "Terjadwal".
- [Single-key shortcuts conflict with assistive technology] → Region scoping, field detection and the off switch (D8).
- [The cycle summary duplicates logic that lives in `routers/dashboard.py`] → The service calls the same helpers (`get_kakeibo_breakdown`, `load_spending_offsets`, reportable SQL) instead of copying the SQL, and a test asserts equality with the overview's Kakeibo total.
- [Phase 1 is the largest phase] → Tasks are grouped so tokens and shell can be reviewed before Beranda. If review shows it is too big, groups 1–3 can ship as their own PR without changing the plan.

## Migration Plan

- No schema migration in phase 1. The new endpoint is additive. `/pulse`, `/dashboard/overview` and the phone are untouched.
- Browser storage: `theme` gains the value `system` (absent means system), and `shortcuts` is new. `sidebarCollapsed` and `hideBalances` keep their keys. Stale `theme_preset` and `theme_palette` keep being removed.
- Release: the owner merges and the existing `deploy.yml` pipeline releases. Backend and frontend ship in the same image set, so no ordering problem exists between them. The companion is unaffected.
- Rollback: revert the phase's merge commit and redeploy through the pipeline. No data needs reverting.

## Open decisions for owner

| # | Decision | Recommended default (used by this plan) |
| --- | --- | --- |
| OD1 | Product name and mark: adopt "Arus" from the preview? | Keep "CashFlow" with a neutral mark until you confirm; swapping is one constant and one SVG |
| OD2 | When the next salary date cannot be estimated from regular receipts, may you enter an expected date yourself? | Not in phase 1: show "Sisa" without a daily figure. Add an owner-entered expected date in Pengaturan > Gaji as a small follow-up if you want it |
| OD3 | Should goal set-asides count in "Terjadwal"? | Yes, but only goals with a target date, as the unfunded part of the even monthly pace, each line explained |
| OD4 | "Kekayaan bersih … sejak awal siklus" delta | Omit until historical valuation exists. Investments are valued at current prices, so a start-of-cycle figure would be invented |
| OD5 | KPI chips as percentages (preview) or amounts | Signed amounts over the same elapsed days, no percentages (keeps the approved fair-comparison rule); no "vs kemarin" chip on Aman |
| OD6 | Delete in Transaksi: undo (needs soft delete and a migration) or confirm first | Confirm first in phase 3; soft delete with "Urungkan" as its own change if wanted |
| OD7 | Wording of the third number: "belum dihitung" is true only for one-sided transfer legs | Lede says "{n} hal nunggu jawabanmu ({amount})", and the explanation shows what is already inside Keluar |
| OD8 | Tanya free-form answers with a model | Phase 6 ships deterministic intents only. A separate LLM change follows only with your approval of provider, budget and privacy terms |

## Open Questions

- The exact tuned pillar hex values and the Geist subset file size are settled in task 1.2 and 1.3 with their measurements. Neither changes the specs.
- Whether "Saran dari riwayat" is its own Tinjau folder or folded into "Perlu jawaban" is settled in phase 2's design.
