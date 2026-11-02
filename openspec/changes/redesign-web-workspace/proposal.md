# Proposal

## Why

The web app grew page by page. Each page has its own header, cards and phone layout, and Beranda answers "how am I doing this cycle?" with charts instead of a few numbers the owner can check. The owner reviewed seven clickable redesign options and approved one combined design: a floating workspace window with a grouped sidebar, a written Beranda whose numbers explain themselves, inbox-style Tinjau, and purpose-built phone layouts in light and dark themes.

The redesign touches every page, so it ships in six phases. This change is phase 1: the shared foundation (tokens, shell, navigation, primitives, theme) and Beranda. design.md holds the plan for all six phases.

## Approval

Recorded 2026-11-02:

1. **Redesign.** The owner approved the combined preview `cash-flow-tracker-prototypes/2026-11-02-web-redesign/combined.html` (sources in `src/combined/`, screenshots in `screenshots/combined-*.png`) as the definition of the full web redesign: "approved, write the implementation plan".
2. **Decisions on the preview, same day:**
   - Savings transfers and investment top-ups count inside "Keluar", as the app does today (the Kakeibo total, which includes net fresh saving).
   - The Beranda summary has exactly three explainable numbers: keluar, aman hari ini and belum dihitung.
   - Analisis keeps the Siklus gaji / Bulan kalender toggle (phase 4).
   - Peta Aliran folds "Masih ada" by default (phase 4).
   - Automatic top-up recording stays on; the backend already does this (phase 5 shows it, nothing changes).
3. **Earlier approvals that still bind:** Phase 0 (2026-11-02): the product is public and multi-user, the design must not be specific to the owner, optional features stay hidden until needed, and amounts are never guessed.

4. **Plan approval, 2026-11-02:** the owner approved this change (proposal, design, specs and tasks) with "approved with defaults, start phase 1". Every open decision is resolved to its recommended default:
   - **OD1** keep the "CashFlow" name with a neutral mark.
   - **OD2** no owner-entered salary date in phase 1. Without a regular salary date Beranda shows "Sisa" without a daily figure.
   - **OD3** goal set-asides count in "Terjadwal" only for goals with a target date, one explained line per goal.
   - **OD4** omit the net-worth delta until a historical valuation exists.
   - **OD5** KPI chips are signed amounts over the same elapsed days, never percentages, and "Aman" has no chip.
   - **OD6** delete in Transaksi asks for confirmation first (phase 3).
   - **OD7** the third number reads "{n} hal nunggu jawabanmu (Rp X)", and its explanation shows what is already inside "Keluar".
   - **OD8** Tanya ships deterministic intents first (phase 6). A model-backed answer needs its own approved change.

Where the preview conflicts with an approved money or salary-cycle rule, this change keeps the conservative behaviour recorded in design.md. "Aman dipakai hari ini" never divides by a guessed salary date.

## Expected Outcome

- Every signed-in page opens inside the new workspace: a floating window on a warm canvas with a grouped sidebar (Harian, AI, Uang, Laporan) and count badges. The top bar holds search, theme, notifications, "+ Catat" and an account menu. At 761–1180 px the sidebar becomes an icon rail. At 760 px and below a bottom tab bar (Beranda, Tinjau, Catat, Transaksi, Lainnya) replaces the sidebar.
- Light, dark and "Ikuti sistem" themes. The choice persists, and pages never flash the wrong theme on load.
- Every existing URL still opens the same page. Pages not yet redesigned keep their content and behaviour inside the new shell, with the new colours and type.
- Beranda opens with a three-sentence summary. It says how much went out this cycle, how much is safe to spend today and how much is still waiting for an answer. Each of the three numbers opens "Kok segini?", which lists the lines that make up the number and links to the matching transactions. Below the summary come a KPI strip, "Uang siklus ini", "Kerjakan dulu", recent activity, Kakeibo, Ketahanan Dana and one fair-comparison insight. Phones get their own single-column Beranda.
- "Aman dipakai hari ini" divides only by a next salary date that the owner's confirmed salary receipts make regular. Otherwise Beranda shows the remaining amount and says why it cannot give a daily figure. It never divides by a guessed date.
- The shell and Beranda pass WCAG 2.2 AA checks (axe, contrast, keyboard, focus, 200% and 400% zoom and reflow) at 1440, 1280, 1024, 768 and 390 px in both themes. The pages not yet redesigned show no new accessibility violation compared with before the change.

## What Changes

- **Design tokens:** a new light and dark token set from the preview (canvas, window, surfaces, ink, lime action, semantic in/out/transfer/investment/warning, Kakeibo pillars, focus, shadows, radii). Compatibility aliases map the old token names, so pages not yet redesigned follow the new palette. Pillar hues are tuned where the preview breaks the existing hue-separation and contrast rules.
- **Type:** self-hosted Geist (sans) replaces Plus Jakarta Sans. Geist Mono stays for code-like text. Tabular figures stay on all money.
- **Shell:** new window frame, grouped sidebar with Tinjau and Laporan badges, collapse to a rail, companion status, Bantuan and Pengaturan. The top bar gets search (⌘K opens Transaksi with the query), theme, notifications, "+ Catat" and an account menu with "Sembunyikan saldo" and "Keluar". On phones, a bottom tab bar and a "Lainnya" sheet. The legacy `BottomNav` and `MobileDrawer` are removed.
- **Period stepper:** moves from the global top bar into the header of the pages that use it (Transaksi, Analisis, Laporan). Beranda always shows the current salary cycle, or the current calendar month when no cycle is confirmed.
- **Shared primitives:** card and card header, KPI strip, pills, segmented control, chips, table, drawer and bottom sheet, the "Kok segini?" explanation popover (a sheet on phones), segmented money bar, toast with undo, keyboard shortcut registry with a help dialog, and empty, loading and error states.
- **Keyboard:** ⌘K/Ctrl+K search, N for Catat, ? for help and Esc to close. Single-key shortcuts can be turned off (WCAG 2.1.4).
- **Beranda:** rebuilt for desktop and phone from the preview. The hero wallet card, metric matrix, cumulative trendline and category donut are removed from Beranda. Due plans keep their link, skip and record actions through "Kerjakan dulu", and the report-ready notice moves to notifications.
- **Backend:** new `GET /api/dashboard/cycle-summary` returns the Beranda figures and their explanation lines: keluar by pillar, masuk, rows waiting for an answer and whether each is already counted, scheduled plan occurrences and goal set-asides, the expected salary date with its basis, safe-to-spend, and net worth parts. The money arithmetic is computed server-side. No schema change.
- **Document language:** the root `html` becomes `lang="id"` (closes the AGENTS.md follow-up).

## Non-goals

- Tinjau triage, Transaksi drawer, Analisis tabs (Ringkasan, Kalender, Peta Aliran), Laporan, Rekening & Kantong, Target & Tagihan, Pengaturan, the text-first Catat dialog and Tanya. They are phases 2–6 in design.md, each its own change and pull request. In phase 1 these pages keep their current content inside the new shell.
- Any change to what the ledger counts. Keluar, Masuk, Kakeibo, balances, reimbursements, movement pairing and review status keep their current meaning. Rows waiting in Tinjau stay recorded and counted as they are today (design.md D9).
- Renaming the product. The preview's "Arus" name and logo are an open decision. Phase 1 keeps the current product name.
- Any LLM feature. Tanya's free-form answers are a later, separate change (design.md, Phase 6).
- The Android companion (`mobile/`) and the `/pulse` contract. `safe_to_spend_today` in `/pulse` and in the overview stays `null`.
- Production. All verification is local, against a disposable migrated PostgreSQL with synthetic data and a local frontend. Pushing the branch, opening the pull request, merging and releasing stay with the owner and the approved pipeline.

## Capabilities

### New Capabilities

- `beranda-cycle-summary`: the Beranda daily summary of the current period. It owns the three-number lede, KPI strip, "Uang siklus ini", "Kerjakan dulu", recent activity, Kakeibo, Ketahanan Dana and insight cards on desktop and phone, the safe-to-spend rule and the `GET /api/dashboard/cycle-summary` contract.
- `explainable-figures`: how an explainable figure behaves anywhere in the web app ("Kok segini?"): trigger, popover or sheet, lines that add up to the figure, notes, drill-down link, keyboard and screen-reader behaviour.

### Modified Capabilities

- `executive-sidebar-layout`: grouped sidebar with badges and companion status, rail at medium widths, floating window instead of a full-bleed workbench. Adds the workspace top bar, phone tab bar and "Lainnya" sheet, route compatibility and global shortcuts.
- `tactile-pulse-ui`: the information architecture follows the new groups and phone tabs. The visual standard moves to the new tokens and Geist with the same contrast and hue rules. Animated headline figures apply to the Beranda KPI strip. The N shortcut follows the shortcut switch.
- `theme-presets-switcher`: removes the obsolete preset switcher. Adds light, dark and system themes with persistence and no flash. Buttons follow the new hierarchy (ink primary, lime positive action).
- `kazz-ui-and-kakeibo`: removes the floating capsule dock, which the phone tab bar replaces.
- `monthly-navigation-stepper`: the stepper moves into the headers of period-based pages, and the top bar contents change.
- `web-font-hosting`: bundled fonts become Geist and Geist Mono.
- `mobile-native-layout`: the phone Beranda composition at 760 px and below.
- `finance-dashboard-cockpit`: the KPI ribbon becomes the Beranda KPI strip. Planning information allows safe-to-spend only from an expected salary date. Removes the Beranda trendline, category donut and hero action pills.
- `observed-insights`: the Beranda insight uses the current salary cycle, or the current calendar month when no cycle is confirmed, because Beranda no longer has a period selector.
- `salary-cycles`: adds the expected next salary date, an estimate from regular confirmed receipts that never becomes a cycle boundary.
- `reimbursement-offsets`: safe-to-spend uses net cycle spending and the expected salary date rule instead of any assumed cycle end.
- `indonesian-daily-finance-ui`: the document language is Indonesian.
- `investment-topups`: the Beranda recent activity is named "Aktivitas terbaru" on desktop and "Hari ini" on phones. The lavender top-up presentation stays.

Standards that apply: web UI and accessibility (WCAG 2.2 AA), money correctness, and multi-user data isolation for the new endpoint. The capability list in AGENTS.md (`database, deployed, money, personal-data, background-jobs, api-consumers, multi-user, llm`) does not change in phase 1. Phase 6 Tanya would add a new LLM use and must say so in its own change.

## Impact

- **Frontend:** `frontend/tokens.css`, `tailwind.config.js`, `src/app/globals.css`, `src/app/layout.tsx` (font, `lang`, pre-paint theme script), `src/components/layout/*` (new shell; `BottomNav` and the `MobileDrawer` part of `Sidebar` removed), new `src/components/workspace/*` primitives, `src/app/page.tsx` and new `src/components/beranda/*`. The `src/components/dashboard/*` components Beranda no longer uses are removed (`HeroWalletCard`, `MetricMatrixGrid`, `CumulativeCashFlowChart`, `CategoryDonutChart`, `ObservedCashFlowCard`, `MobileHomeView`, `NarrativeInsightCard`) and their tests are replaced. `EmergencyCoverage` and `KakeiboBenchmark` stay for Rekening and Analisis. Tests: `tokens.test.ts`, `fonts.test.ts`, layout tests and the Beranda tests are updated or replaced.
- **Backend:** `backend/app/routers/dashboard.py` (new route), a new `backend/app/services/cycle_summary.py`, `backend/app/services/salary_cycles.py` (expected date), and tests. No migration. `/pulse` and `/dashboard/overview` keep their responses.
- **Fonts:** adds the Geist variable woff2 with its OFL licence. Removes the Plus Jakarta Sans files.
- **Docs:** AGENTS.md "Visual and language references" (tokens source, `lang` follow-up closed) and `docs/NEW_USER_GUIDE.md` screenshots or wording where they describe Beranda or navigation.
- **No new runtime dependencies** in phase 1.
