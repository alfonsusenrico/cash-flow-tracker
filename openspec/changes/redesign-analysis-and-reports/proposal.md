# Redesign Analisis and Laporan

## Why

Phase 4 of the approved web redesign turns Analisis and Laporan into workspace pages built on the shared primitives. Today:

- **Analisis** (`/insights`, 1019 lines) is a legacy page with two tabs, "Ringkasan & Anggaran" and "Bandingkan & Kakeibo". It stacks charts, a weekday heatmap and the category forms. Nothing on it shows money by day or where the money of a period went.
- **Laporan** (`/laporan`) still uses the legacy header and its own "Laporan lain" picker, which phase 1 left in place until this phase.

The approved preview gives Analisis three tabs: Ringkasan, Kalender and Peta Aliran. It also gives Laporan a written lead and a list of past reports.

The owner wants the redesign finished before testing anything ("I'm not going to test anything until all phases done implemented properly and approved", 2026-11-03). So the checks in this plan are the only runtime evidence before acceptance.

## Approval

Binding earlier approvals:

1. **Phase plan, 2026-11-02.** The owner approved `redesign-web-workspace` with "approved with defaults, start phase 1". This phase follows its design:
   - the P4 row: Analisis tabs Ringkasan, Kalender and Peta Aliran with the Siklus gaji / Bulan kalender toggle; Kalender heat, day sheet, bills timeline and phone week strip; Peta Aliran Sankey with keyboard node navigation, a table alternative, a drawer, replay and "Masih ada" folded by default; the Laporan restyle;
   - the draft contracts for `GET /dashboard/calendar` and `GET /dashboard/flow`;
   - D7 (shared primitives), D8 (one shortcut registry and WCAG 2.1.4), D9 (waiting rows stay recorded and counted), D13 (no money arithmetic in the client), D14 (sub-views as query parameters on `/insights`), D17 (WCAG 2.2 AA verification) and D21 (port the preview's Sankey layout, no new dependency).
2. **Phase 1 decisions that apply here.**
   - OD1: the product keeps the name "CashFlow".
   - OD7: waiting items are never worded as left out of totals.
   - The Laporan picker moves out of the page body in this phase (phase 1 placement notes).
3. **Combined preview, 2026-11-02.** The owner approved `cash-flow-tracker-prototypes/2026-11-02-web-redesign/combined.html` as the definition of the redesign. Its Analisis tabs and Laporan view are the visual target. Where it conflicts with an approved money rule, the rule wins, as recorded in design.md, "Preview conflicts".
4. **Phase 3, 2026-11-03** (`redesign-transactions-workspace`, "approved, defaults ok"). This phase reuses:
   - the shared `TransactionDrawer` and the `Drawer` primitive;
   - the `lib/urlState.ts` history-API writer (D13 there), because the Next.js router defect also affects `/insights`;
   - the "Belanja" versus "Keluar" naming (OD24);
   - the copy register (D20 and `ui-copy-register`).
   Phase 3 merges before this phase is implemented, and the implementation branch is rebased onto that `main`.
5. **Copy register, 2026-11-03.** The owner said: "for all labels shown in the app it should be not that informal terms or language for example "Udah dicek" it's good for onboarding interactive messages, but for label or term used in the UI should not use that kind of terms. I hope you can tell the difference and decide which is when". The lead's rule (phase 3 D20) is binding for this phase.
6. **Planning of phases 4 to 6, 2026-11-03.** The owner said "yes, plan P4–P6 now". This authorises planning. It does not approve this plan.

**Plan approval, 2026-11-03.** After a combined summary of the phase 4, 5 and 6 plans, the owner approved all three with "approved, defaults ok". OD40–OD48 take their recommended defaults (design.md, open decisions).

## Expected Outcome

- **Analisis tabs.** Analisis has three tabs, Ringkasan, Kalender and Peta Aliran, kept in the URL (`/insights?tab=kalender`). A Siklus gaji / Bulan kalender toggle and the period stepper apply to all three tabs.
- **Ringkasan.**
  - A figure strip: Masuk; Keluar, equal to Beranda's Keluar; Belanja, equal to Transaksi's Belanja.
  - Kakeibo against the same day of the previous period, with saving counted as Beranda counts it.
  - Belanja harian bars with the peak marked.
  - The detected bills and subscriptions, how often the app asked, the net cash flow history, and the category and budget section that is already on the page.
- **Kalender.**
  - A month-style grid of the period. Each day shows its spending, written in the cell and shaded by level.
  - Markers for income, Pindah saldo, Top up Investasi, waiting rows and due bills.
  - A day sheet with that day's transactions, waiting rows and bills.
  - A list of the bills due before the next salary.
  - On the phone, a week strip and the day list.
  - The days add up exactly to the period's Belanja.
- **Peta Aliran.**
  - A Sankey of the period: sources into accounts, accounts into Kakeibo pillars, Tabungan, investments and other accounts, and what is left ("Masih ada", folded by default).
  - Every node is reachable with the keyboard.
  - A complete table lists every link.
  - A drawer lists the transactions behind a node or link.
  - Replay shows the period day by day.
  - For every account node, the opening balance plus inflows equals outflows plus "Masih ada".
  - Rows waiting for a counterpart have their own node.
- **Laporan.**
  - The period stepper and basis toggle move to the top bar.
  - The page opens with a short written lead in neutral Indonesian built from the report's own figures.
  - The four questions, comparison, data quality, budget and reflection stay.
  - Past reports are listed beside the report on desktop and below it on the phone.
- **Both pages follow the neutral label register.** Formal "Anda" and informal words on them are rewritten.

## What Changes

**Backend (additive):**
- **New `GET /api/dashboard/calendar`:** per local day of a cycle or month, spending, the markers, the waiting rows and plan occurrences, plus the upcoming bills and the expected salary date.
- **New `GET /api/dashboard/flow`** with an optional `through` day for replay: nodes, links and per-account conservation figures.
- **New `GET /api/dashboard/flow/rows`:** the transactions behind one node or link, paginated, from the same rule code as the link.
- **`GET /api/dashboard/period-comparison`** gains a `kakeibo` block: Kakeibo buckets, including net fresh saving, for both compared windows.
- **`GET /api/dashboard/analytics`** gains `explanations` for Keluar and Belanja, as server lines.
- **The waiting and plan-line helpers move** from `cycle_summary.py` into shared services, so Beranda, the calendar and the flow count the same rows.

**Frontend:**
- **`/insights`** is rebuilt from `components/analysis/*`: tabs, toolbar, Ringkasan, Kalender, Peta Aliran and the Sankey module ported from the preview's `sankey.js` to TypeScript.
- **The category and budget section** moves out of the page into `components/analysis/CategoryBudgetSection.tsx` with unchanged behaviour.
- **The weekday heatmap card is removed** (OD43). The API field stays.
- **`/laporan`** is restyled on the primitives: top-bar controls, the written lead (`lib/reportLead.ts`), the past-reports card, and the existing answer, comparison, data-quality, budget, reflection and classification components restyled.
- **Docs:** `docs/NEW_USER_GUIDE.md` describes the three tabs and the Laporan lead.

## Non-goals

- **The "Kebiasaan dihafal" card.** It needs `GET /api/habits`, which phase 5 adds (OD42).
- **Moving category and budget management to Pengaturan** (OD40).
- **New money rules.** Belanja, Keluar, Masuk, the reimbursement offsets and the Kakeibo buckets keep their current definitions. Waiting rows stay counted (D9).
- **Exporting, printing or sharing reports.**
- **Changing report periods, readiness, reflection storage or the classification flow.**
- **Any LLM use.** Every figure and sentence comes from deterministic server values.
- **Uang pages, Pengaturan and Tanya** (phases 5 and 6).
- **The Android companion.** It reads only `/dashboard/cash-flow-summary`, which this change does not touch.

## Capabilities

### New Capabilities

- `analysis-workspace`: the Analisis page. It covers the tabs and their URL, the period basis toggle, the Ringkasan composition and figure names, the category section's placement, the phone layout and accessibility.
- `cycle-calendar`: the calendar endpoint, its conservation rule, and the Kalender tab with its day sheet, bills timeline and phone week strip.
- `money-flow-map`: the flow and rows endpoints, the per-account conservation rule, waiting nodes, replay, and the Peta Aliran tab with keyboard navigation, the table alternative and the drawer.

### Modified Capabilities

- `monthly-report`: "Report page layout" moves the controls to the top bar and adds the past-reports card. A new requirement covers the written lead and its register.
- `observed-insights`: "Fair period comparison" adds Kakeibo buckets with net fresh saving for both windows.

This change adds no project capability in the `AGENTS.md` sense: no new database, deployed service, personal-data store, background job, API consumer or LLM use. The endpoints are owner-scoped reads on the existing multi-user API.

## Impact

- **Backend:**
  - `backend/app/routers/dashboard.py` (three routes, two additive fields);
  - new `backend/app/services/calendar.py` and `backend/app/services/money_flow.py`;
  - `backend/app/services/cycle_summary.py` (helpers moved to `backend/app/services/waiting_rows.py` and `plan_occurrences.py`);
  - `backend/app/services/observed_insights.py`;
  - tests under `backend/tests/`.
- **Frontend:**
  - `frontend/src/app/insights/page.tsx` and `frontend/src/app/laporan/page.tsx`;
  - new `frontend/src/components/analysis/*` and `frontend/src/lib/reportLead.ts`;
  - `frontend/src/components/report/*` (restyle), `frontend/src/components/insights/*` (restyle), `frontend/src/lib/queryKeys.ts` and `frontend/src/types/*`;
  - removal of `components/dashboard/SpendingHeatmap.tsx` and of `BurnCadenceChart` if it is unused afterwards.
- **Database:** none. No migration.
- **API:** three new owner-scoped GET routes and additive fields on two routes. Nothing is removed or renamed.
- **Dependencies:** none added. recharts already ships on `/insights`.
- **Shared files with other phases:** `globals.css`, `lib/queryKeys.ts`, `docs/NEW_USER_GUIDE.md`, and `components/workspace/*` (read only). Overlap with phases 5 and 6 is listed in design.md, "Risks".
