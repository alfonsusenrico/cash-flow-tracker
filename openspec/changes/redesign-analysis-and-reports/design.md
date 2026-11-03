# Design: Redesign Analisis and Laporan

## Context

### Analisis today (observed at `861212b`)

- **`frontend/src/app/insights/page.tsx` (1019 lines)** reads the shared `timeframe` and `cycleOffset` from `AppContext`. It renders the period stepper through `TopBarControls`, a "Siklus gajian / Bulan kalender" toggle and two tabs held in local state.
  - **"Ringkasan & Anggaran"** has three cash-flow tiles, then:
    - `BurnCadenceChart` (recharts), fed by `GET /dashboard/analytics` `daily_cadence`;
    - `SpendingHeatmap` (weekday), fed by `day_of_week_heatmap`;
    - the category variance table with inline budget editing;
    - the income and archived category lists;
    - a category create or edit `Modal` with name, icon, colour, budget, kind and pillar.
  - **"Bandingkan & Kakeibo"** has `PeriodComparison`, `CashFlowHistory`, `RecurringCharges` (with "Jadikan rencana" into `RecurringRulesModal`), `QuestionRate` and `KakeiboBenchmark`.
  - The tab is not in the URL.
- **Specs that pin this page's content:**
  - `custom-category-management` and `finance-dashboard-cockpit` require the category budget table and its inline editing on `/insights`;
  - `habit-learning` requires the "Seberapa sering ditanya" panel on Analisis;
  - `observed-insights` defines the comparison, history and recurring charges.
- **Rolling windows (`30d`, `90d`)** exist in the API and in `ReportTimeframe`. No control in the interface sets them.

### Laporan today

- **`frontend/src/app/laporan/page.tsx` (206 lines)** uses the legacy `PageHeader`. It holds the basis switch (Bulan kalender / Siklus gaji) and a `<select>` "Laporan lain" in the body.
  - It renders `DataQualityPanel`, `IncomeAnswer`, `SavingsAnswer`, `SpendingAnswer` and `ReflectionAnswer`.
  - Beside them sit `ComparisonPanel` and `BudgetPanel`, plus `ClassificationDialog` and `BudgetEditorDialog`.
  - It writes `basis` and `start` with `router.replace`, which phase 3 OD21 moves to `lib/urlState.ts`.
- **The report has no written lead.** `GET /reports/monthly` returns figures, answers and the comparison.

### Money rules this phase reads (observed)

- **Belanja (spending):**
  - `reportable_expense_sql`: an expense that is not a movement, not an investment and not in an excluded category;
  - minus the applied reimbursement offsets (`load_spending_offsets`, `cash_flow_totals`);
  - `daily_offsets` already spreads the applied offsets over local days, so `daily_cadence` adds up to `total_outflow`.
- **Keluar (Beranda, phase 1 D9):** the Kakeibo `total_allocated`, i.e. the pillars net of offsets plus net fresh saving through linked movements into investment or goal accounts.
- **The fair comparison (`observed_insights`)** gives pillar spending from reportable expenses only, so its `saving` excludes saving transfers.
- **The ledger balance of an account** is `initial_balance` plus income minus expense over all its rows (`get_accounts_with_balances`). A parent's balance adds its pockets.
- **Waiting rows:**
  - they are counted by `REVIEWABLE_ROW_SQL AND ATTENTION_SQL`;
  - `cycle_summary._waiting` gives each one a Tinjau section: questions, ambiguous, awaiting or groups;
  - an awaiting row is an Internal Movement leg without its counterpart.
- **Plan occurrences in a window:** `cycle_summary._plan_lines` returns the unpaid, linked and skipped occurrences.
- **The companion** calls only `/dashboard/cash-flow-summary` (sibling repository, read at `35dfbeb`).

## Goals / Non-Goals

**Goals:**
- Build the three Analisis tabs and the Laporan restyle on the phase 1 primitives and the phase 3 drawer.
- Add exact, tested contracts:
  - calendar days add up to Belanja;
  - every account node of the flow balances;
  - drawer rows add up to their link;
  - waiting rows are visible and never silently moved.
- Meet WCAG 2.2 AA: the map is reachable with the keyboard, its table is complete, and the calendar is a navigable grid.
- Keep the companion and every existing route and parameter working.

**Non-Goals:** see proposal.md, "Non-goals". In particular, no money rule changes, no new dependency and no migration.

## Decisions

### D1. Page frame, tabs and URL

- **Tabs:** `/insights` renders a `role="tablist"` with Ringkasan, Kalender and Peta Aliran, using roving tabindex, arrow keys, Home and End.
  - Each tab is `?tab=ringkasan|kalender|aliran` (phase plan D14).
  - A missing or unknown value means Ringkasan, so `/insights` and every existing link keep working.
  - The selected calendar day is `&day=YYYY-MM-DD`, and the flow view mode is `&view=peta|tabel`.
  - All URL writes go through `lib/urlState.ts`'s `replaceSearch`, because phase 3 reproduced the Next.js router defect on `/insights?x=1` → `/insights`.
- **Toolbar:** it holds the period basis `Segmented` (Siklus gaji, Bulan kalender), bound to `AppContext.timeframe`. The period stepper stays in the top bar through `TopBarControls`.
  - Both apply to all three tabs.
  - If `timeframe` is a rolling value (not reachable in the interface today), Analisis shows the cycle and says so in the toolbar caption, without writing the shared value.
- **Rendering:** each tab panel mounts only when selected, and its query starts only then.
  - The Kalender and Peta Aliran code is loaded with `next/dynamic`, so Ringkasan's first load does not carry the map.

*Rejected:*
- Separate paths (`/insights/kalender`), because D14 keeps sub-views as query parameters.
- Rendering all three panels hidden, which would cost three times the requests and the JavaScript.

### D2. Ringkasan composition and figure names

- **Desktop:**
  - The `KpiStrip` holds Masuk, Keluar and Belanja, each an `ExplainableFigure`:
    - Masuk is the overview inflow;
    - Keluar is Kakeibo `total_allocated` and equals Beranda's Keluar for the current period;
    - Belanja is `total_outflow` and equals Transaksi's Belanja (phase 3 OD24).
  - A caption under the strip reads "Keluar = Belanja ditambah yang ditabung". It is worded as a rule, and the amounts come only from the explanations.
  - Then two columns:
    - Left: "Kakeibo" (D3), "Belanja harian" (D4) and "Riwayat arus kas" (`CashFlowHistory`).
    - Right: "Tagihan & langganan terdeteksi" (`RecurringCharges`, with "Jadikan rencana" unchanged), "Seberapa sering ditanya" (`QuestionRate`) and the comparison's increases and decreases (`PeriodComparison`, restyled).
  - Below the columns: "Kategori & anggaran" (D5).
- **Columns:** at a main column of 900 px or less, the cards form one column in the order above. This uses the container query plus the `useWidthAtMost` hook pattern from phase 1 D16, so DOM order follows the layout.
- **Explanations:**
  - `GET /dashboard/analytics` gains `explanations: {keluar, belanja}`, built by the server with the phase 1 explanation shape (`lines`, `total`, `caveat`, `href`), so the client does no arithmetic (D13).
  - Keluar lists the six buckets.
  - Belanja lists gross spending and the applied reimbursement offset.
- **Phone (≤ 760 px):** one column. The order is figures, Kakeibo, Belanja harian, Tagihan, Seberapa sering ditanya, Riwayat arus kas and Kategori & anggaran. KPIs become a two-by-two grid with the third figure full width.

*Rejected:*
- Keeping "Uang keluar" for `total_outflow`. It is the same word as Beranda's Keluar for a different number, which phase 3 OD24 resolved.
- Showing Bersih (net). The preview has no net on Ringkasan, and the comparison card already states it.

### D3. Kakeibo against the same day of the previous period (OD41)

- **Contract:** `GET /dashboard/period-comparison` gains an additive `kakeibo` block, `{current: {bucket: amount}, previous: {bucket: amount} | null}`.
  - The buckets are need, want, culture, unexpected, saving and unclassified.
  - Each is computed by `get_kakeibo_breakdown` for the compared windows that `pillars` already uses, so saving includes net fresh saving.
  - `current` for the current window equals the analytics `kakeibo` of the same window.
  - The existing `pillars` entries stay unchanged for their current readers.
- **Card:** one bar per bucket, with a marker at the previous value and a pill "Naik Rp X" or "Turun Rp X".
  - It shows no percentages (`observed-insights`: no percentage change).
  - Unclassified with spending reads "Perlu dipilih" and links to Tinjau or Laporan's "Lengkapi klasifikasi".
  - In Bulan kalender, the caption names both compared windows ("1–2 Nov dibanding 1–2 Okt").
  - The bars carry `role="img"` with an accessible name naming both amounts.

*Rejected:*
- Using `pillars`, because its saving leaves out transfers into savings and would contradict Beranda's Keluar.
- Computing the previous window in the client.

### D4. Belanja harian

- **Data:** `daily_cadence` from `/dashboard/analytics` (unchanged).
- **Bars:** one bar per local day. The highest day is filled in the accent and labelled "Tertinggi: {date} {amount}", today is outlined and labelled "hari ini, masih berjalan", and days after today are absent.
- **Implementation:** CSS and SVG bars in a small component, with no recharts, so the page drops `BurnCadenceChart` from its bundle if nothing else uses it.
  - Each bar has a text alternative.
  - A `<table>` is reachable with "Lihat sebagai tabel" for screen readers and zoom.

*Rejected:* keeping recharts here. The bars are simple, and its tooltip and keyboard behaviour do not meet D17 without custom work.

### D5. Category and budget section (OD40)

- **Move:** the category variance table, inline budget editing, the income and archived lists and the category form move unchanged in behaviour into `components/analysis/CategoryBudgetSection.tsx`. It renders as Ringkasan's last card, "Kategori & anggaran".
  - The mutations, query keys and validation are unchanged.
  - The `Modal` is replaced by the native dialog primitive.
  - The `custom-category-management` and `finance-dashboard-cockpit` requirements stay satisfied on `/insights`.
- **Copy:** user-facing strings follow the register, e.g. "Klik untuk mengatur batas anggaran" → "Atur batas anggaran".
- **Home:** phase 5 may move this section to Pengaturan. That move would change those two specs, and belongs to phase 5's plan.

### D6. Calendar contract (`GET /api/dashboard/calendar`)

**Request:** `timeframe=cycle|month` (`PERIOD_PATTERN`) and `cycle_offset` (−24 to 12, as analytics). It is owner-scoped. An invalid value answers 422.

**Response:**

```json
{
  "window": {"start": "2026-10-25", "end": "2026-11-24", "label": "Siklus 25 Okt – 24 Nov",
             "today": "2026-11-02", "day_index": 9, "days_total": 31, "is_current": true},
  "expected_salary": {"date": "2026-11-25", "available": true, "reason": null},
  "totals": {"spend": 1834000, "gross_spend": 1874000, "offset_applied": 40000},
  "days": [{
    "date": "2026-11-02", "spend": 109000, "spend_count": 3, "heat": 2,
    "income_total": 0, "income_count": 0,
    "movement_total": 500000, "movement_count": 1,
    "topup_total": 0, "topup_count": 0,
    "waiting_count": 1, "waiting_amount": 45000,
    "plans": [{"rule_id": "…", "label": "Internet Rumah", "amount": 350000, "status": "due"}]
  }],
  "upcoming": [{"date": "2026-11-05", "rule_id": "…", "label": "Internet Rumah", "amount": 350000, "status": "due"}],
  "upcoming_total": 350000,
  "heat_thresholds": [50000, 120000, 250000, 400000]
}
```

- **`days`:** covers every local day of the window, including future days of the current period. Future days have zeros and only `plans`.
- **`spend`:** reportable expense on that day minus `daily_offsets` for that day. This is the same code path as `daily_cadence`. **Invariant:** `Σ days.spend = totals.spend = analytics.total_outflow` for the same window, and `totals.gross_spend − totals.offset_applied = totals.spend`. A day is never negative, because `daily_offsets` apportions within each category's days.
- **Other markers:**
  - `income_*` counts reportable income;
  - `movement_*` counts linked movement pairs, once per pair on the outbound leg's local day, excluding top-ups;
  - `topup_*` counts top-up movements, i.e. inbound legs on investment products;
  - `waiting_*` counts the Tinjau attention rows by their local day, with the same SQL as the badge (`ATTENTION_SQL`);
  - `plans` holds the plan occurrences falling on the day, from the shared plan-line helper, with `status` `due|overdue|met|skipped`.
- **`upcoming`:** only for the current period (`is_current`). It holds the unmet occurrences from today to the expected salary date, or to the window end when that date is unavailable. It is ordered by date and is the "Sebelum gaji berikutnya" list.
- **`heat` (0–5):** 0 for no spending. Levels 1–5 come from `heat_thresholds`, the 20th, 40th, 60th and 80th percentiles of the window's non-zero days, rounded to Rp 1.000 by the server (OD47). The client only maps the level to a token colour, and the amount is always written in the cell.
- **Statement count:** constant, at most 8, whatever the window length. It is tested.

*Rejected:*
- Gross day figures. They would not add up to Belanja, and `daily_offsets` already solves the split.
- Client-side heat levels, which would be money arithmetic in the client.

### D7. Kalender tab

- **Desktop:**
  - **Progress band:** "Hari ke-9 dari 31" with "16 hari lagi sampai gaji 25 Nov", or "Tanggal gaji berikutnya belum bisa diperkirakan" with the reason.
  - **Grid:** a `role="grid"` of weeks starting Monday (OD46).
    - Every cell is a `gridcell` holding a button, with roving focus and arrow keys, Home and End per week, and PageUp and PageDown for the previous and next week.
    - A cell shows the day number, `spend` (or nothing for 0), a heat fill from the level, and icon markers with text alternatives: Masuk, Pindah saldo, Top up, Menunggu jawaban n and Tagihan.
    - The accessible name reads, e.g., "Senin 2 November: belanja Rp 109.000, 1 menunggu jawaban, tagihan Internet Rumah".
    - Days outside the window are blank and inert.
  - **Legend:** it names the levels with their threshold amounts.
  - **Side column:** the day sheet and the upcoming list.
- **Day sheet** for the selected day, `day` in the URL, defaulting to today in the current period and the last day otherwise:
  - The transactions come from `GET /transactions?from_date=D&to_date=D&limit=50`, using phase 3's list API with its `daily_totals`. Each row opens the shared `TransactionDrawer`, and "Lihat di Transaksi" opens `/ledger?from=D&to=D`.
  - "Menunggu jawaban" lists the waiting rows of the day, linked to `/review?item=<id>`.
  - "Tagihan" lists the day's plans with their status.
  - The heading says the day's Belanja from `spend`.
- **Upcoming:** "Sebelum gaji berikutnya", with the total from the server's list. The server returns `upcoming_total`, because the client does not add money. Each row opens Target & Tagihan's plan.
- **Phone (≤ 760 px):**
  - a week strip of seven day buttons with spending under each, plus "Minggu sebelumnya" and "Minggu berikutnya" buttons;
  - the day sheet as the page body, not a bottom sheet, so it scrolls with the page;
  - the upcoming list after it.
  - The week strip is a labelled group of `aria-pressed` buttons, not a tab list, because each day is a selection, not a tab.

*Rejected:* a recharts calendar or heatmap. A CSS grid gives the grid semantics D17 needs.

### D8. Flow contract (`GET /api/dashboard/flow`)

**Request:** `timeframe=cycle|month`, `cycle_offset` and an optional `through=YYYY-MM-DD`.
- `through` defaults to the window end, or to today for the current period.
- A `through` outside the window answers 422 `through_outside_window`. The client clamps its slider, so this happens only for a crafted request.
- The route is owner-scoped.

**Nodes:** `{id, kind, label, rank, account_id?, pillar?, folded_by_default?}`. The kinds:

- **`source`:**
  - one per reportable income category, labelled with the category name (for example "Gaji");
  - "Penggantian uang" for income that offsets spending;
  - "Masuk lainnya" for other non-reportable income that is not a movement leg;
  - "Saldo sebelum periode" (`folded_by_default`);
  - "Saldo minus di akhir" for an account that ends below zero (see below).
- **`negative_opening`:** "Saldo minus sebelum periode", on the outgoing side, for an account that starts below zero.
- **`account`:** one per liquid top-level account that is not archived. Its non-goal pockets are folded in (OD44). A liquid account is `type <> 'investment' AND instrument_type IS NULL` and not linked to a goal. An archived account with rows in the window is included.
- **`saving`:** one per goal-linked account (pocket or account), labelled with the goal and account name.
- **`investment`:** one per top-level investment account, with its products folded in.
- **`pillar`:** need, want, culture, unexpected, saving and unclassified, labelled with the Kakeibo names.
- **`other_out`:** "Tidak dihitung belanja", for expenses in excluded categories that are not movement legs (OD48).
- **`waiting`:** "Menunggu pasangan", for Tinjau awaiting and ambiguous legs (D9). One node takes outgoing legs on the right and incoming legs on the left. In the rendering it appears twice, once per side, under the same id with a side suffix.
- **`remaining`:** "Masih ada" (`folded_by_default`).

**Links:** `{key, from, to, kind, amount, count, waiting_count, waiting_amount}`.
- The `kind` is one of `income`, `opening`, `negative_opening`, `negative_closing`, `transfer`, `spend`, `saving`, `investment`, `other`, `waiting` or `remaining`.
- `amount ≥ 0` and is an integer.
- `key` is a stable string such as `acc:<uuid>>pillar:need`.

**How rows become links.** Each transaction row on an account node is assigned to exactly one link by kind:

| Row | Link |
| --- | --- |
| Reportable income | source → account |
| Expense | account → pillar by `pillar_bucket_sql` if reportable, else account → `other_out` |
| Linked movement between two account nodes | account → account (one link per ordered pair, the pair counted once) |
| Movement into or out of a saving or investment node | account → saving/investment, or saving/investment → account |
| Movement between a parent and its own folded pocket, or between two pockets of one node | internal to the node: appears in no link, and both legs cancel in the node equation |
| Awaiting or ambiguous leg | account → waiting (outgoing) or waiting → account (incoming) |

- **Opening:** "Saldo sebelum periode" → account carries the ledger balance at the window start.
- **Closing:** account → "Masih ada" carries the ledger balance at the end of `through`.
- **Negative balances:**
  - A negative opening appears as account → "Saldo minus sebelum periode" with the absolute amount, on the outgoing side.
  - A negative closing appears as "Saldo minus di akhir" → account, on the incoming side.
  - So every link stays non-negative.

**Conservation (tested for every account node):** `Σ amount(links into node) = Σ amount(links out of node)`. This is equivalent to "opening + inflows = outflows + Masih ada", where the closing equals `get_accounts_with_balances`' ledger balance at the end of `through`, and equals the account's current ledger balance when `through` is today and no later row exists.

**`summary`, server-computed:**
- `{spend_gross, saving_net, investment_net, waiting_out, waiting_in, remaining}`;
- `belanja` and `keluar` equal to the analytics figures for the full window when `through` is the window end;
- `masuk`.
- The four cards under the map read from it, so the client adds nothing.

**Waiting rows inside pillar links.** Rows that wait for a question or a category answer (Tinjau sections "questions" and "groups") stay in their pillar link (phase plan D9), with their count and amount in `waiting_count` and `waiting_amount`. The link is drawn hatched, and its table row says "termasuk n menunggu jawaban".

**Statement count:** constant, at most 10, whatever the accounts and days. It is tested.

*Rejected:*
- **Netting reimbursement offsets into pillar links.** Offsets are per category and window, so a node would no longer balance. Penggantian appears as a source, and the summary states Belanja net.
- **Per-pocket account nodes.** Too many nodes on a phone, and parent ↔ pocket moves would dominate the map. The drawer lists pockets.
- **Dropping movements between the owner's accounts.** The preview shows them, and they are needed for each node to balance.

### D9. Waiting nodes and the drawer rows (`GET /api/dashboard/flow/rows`)

- **Waiting legs:** a leg in the Tinjau sections "awaiting" or "ambiguous" never appears as spending or saving. It goes to the "Menunggu pasangan" node, whose drawer offers "Jawab di Tinjau" (`/review?folder=pindah`, the phase 2 folder key).
- **Rows request:** `GET /api/dashboard/flow/rows?timeframe&cycle_offset&through&link=<key>|node=<id>&offset=0&limit=50` returns `{rows: [ledger row shape], total_count, total_amount}`.
  - `total_amount` equals the link's `amount` (tested for every link kind of the fixture).
  - A node request returns the rows of all its links.
  - Opening and remaining links return no rows. Their drawer shows the balance explanation instead: opening, plus "Lihat semua transaksi rekening ini" to `/ledger?account=<id>&period=…`.
  - The same assignment function serves `flow` and `rows`, so the two cannot disagree.
- **Errors:** an unknown key answers 404 `flow_link_not_found`, a key of another owner's account also answers 404, and a malformed key answers 422.

*Rejected:*
- Ledger query strings per link. The ledger cannot filter "movements from A to B" or the waiting legs exactly.
- Embedding ids in the flow response, which is unbounded for pillar links.

### D10. Peta Aliran tab

- **Sankey module:** `components/analysis/flow/sankey.ts` is a typed port of the preview's `sankey.js` layout (phase plan D21): columns, node stacking, link ordering and ribbon paths. `FlowMap.tsx` renders it as SVG with token colours, and patterns for Pindah saldo (stripe), Top up (dots) and Menunggu (hatch), so colour is never the only carrier.
- **Keyboard (D17):**
  - The map is a `role="group"` named "Peta aliran uang" (not `role="application"`, so screen-reader browse keys keep working). It has a roving focus over node buttons (`<g role="button" tabindex>` with `aria-label` "Bank Utama: masuk Rp X, keluar Rp Y").
  - Up and Down move within a column, Left and Right move to the nearest node in the adjacent column, Enter or Space opens the drawer, and Esc returns focus to the map.
  - Links are not focusable in the map. They are complete in the table.
  - No single-key page shortcuts are added, so D8's registry is unchanged.
- **Table alternative:**
  - "Peta" and "Tabel" are a `Segmented` (`view=`).
  - The table is a `<table>` with a caption, and columns Dari, Ke, Jenis, Jumlah and Transaksi.
  - It always lists every link, including folded ones, in descending amount. A waiting part says "termasuk n menunggu jawaban".
  - Each row's "Dari → Ke" is a button opening the drawer.
  - On the phone (≤ 760 px) the table is the primary view. A small map above it is `aria-hidden` and decorative.
- **Highlight:** the chips Semua, Belanja, Pindah saldo, Tabungan & investasi and Menunggu pasangan (single select) dim the other links. They are buttons with `aria-pressed`.
- **"Masih ada" folded (default on):**
  - Opening and remaining links are hidden from the map, and node heights follow the visible links.
  - A button "Tampilkan Saldo dan Masih ada · Rp X" shows them.
  - A caption says that the map shows only money that moved, and that the table lists everything.
  - The fold state is per viewer in `localStorage`, wrapped in try/catch.
- **Replay:**
  - A play or pause button and a range input over the window's days up to today. The range is labelled "Putar ulang per hari", with `aria-valuetext` such as "Senin 2 November, hari ke-9".
  - Each step requests `flow?through=D`, cached by TanStack Query per day, with the next day prefetched.
  - Playing advances one day per 800 ms after the previous response (OD45).
  - With reduced motion, playing is off and the slider still steps.
  - An `aria-live="polite"` region announces the day.
  - The node layout keeps the full-window order, so nodes do not jump between days.
- **Drawer:** the shared `Drawer` (side, overlay or sheet by width).
  - **For a node:** the totals in and out, the links in each direction, and for accounts the opening and closing with their explanation.
  - **For a link:** amount, count, waiting part, and the first 50 rows from D9 with "Muat lagi". Each row opens the `TransactionDrawer` (phase 3), and the `Drawer` primitive returns focus on close.
- **Under the map:** four figures from `summary`: Belanja (gross, with "sudah termasuk penggantian Rp X" pointing to the source), Ditabung & top up, Menunggu pasangan, and Masih ada. Waiting rows inside pillar links are counted where they stand.

*Rejected:*
- `d3-sankey` or recharts Sankey (phase plan D21).
- Canvas rendering, which has no element semantics.

### D11. Laporan

- **Top bar:**
  - The basis `Segmented` (Bulan kalender, Siklus gaji) and a report stepper through `TopBarControls`.
  - The stepper's previous and next buttons move along `GET /reports/monthly/periods` for the basis, and its label is the period label.
  - The "Laporan lain" `<select>` is replaced by the stepper and the past-reports card.
  - `basis` and `start` stay the URL contract and are written with `replaceSearch` (phase 3 OD21).
- **Desktop composition:**
  - Main column: the written lead (D12), the four questions in their fixed order, then the reflection.
  - Side column: `ComparisonPanel`, "Kualitas data", the budget, and "Laporan sebelumnya", a list of the periods with readiness, each linking to its report.
  - At a main column of 900 px or less, the side cards follow the questions in the order the `monthly-report` spec already requires on the phone.
- **Components:** the existing report components are restyled on `Card`, `Pill`, `ExplainableFigure` where a figure has server lines, and `EmptyState` and `ErrorState`. Their data and wording rules stay. `ClassificationDialog` and `BudgetEditorDialog` move to the native dialog primitive.
- **States:**
  - loading as skeletons;
  - an error with "Coba lagi";
  - no report yet ("Laporan pertama muncul setelah bulan pertama tercatat", as the spec requires);
  - an incomplete period, as `periodNote` does today.

### D12. The Laporan written lead and its register

- **Builder:** `lib/reportLead.ts` builds two or three sentences from report fields only.
  - The period label and completeness;
  - Masuk and Belanja for the period;
  - the pillar with the largest change against the previous period, when the report has a comparison;
  - the number of rows still unclassified, with a link to "Lengkapi klasifikasi".
  - The money values are server values passed to `fmtMoney` or `bal`, and the function adds or subtracts nothing.
  - Each figure in the lead is an `ExplainableFigure` when the report carries its lines, and plain text otherwise.
- **Register: neutral standard Indonesian.** The lead is built impersonally, e.g. "Oktober: masuk Rp 8.200.000, belanja Rp 4.910.000. Pengeluaran Keinginan turun Rp 310.000 dibanding September. 2 transaksi belum diklasifikasi."
  - **Why neutral:** a report is a record that is re-read and compared over months, not a conversation.
  - The `monthly-report` spec forbids verdicts, and neutral wording keeps it from sounding like one.
  - The owner's rule keeps the casual voice for conversation, and the Beranda lede is the daily greeting that the rule names explicitly.
  - The Beranda lede keeps its voice; Laporan's lead does not.
- **Unavailable values:** a sentence whose figure is unavailable is left out, never filled with Rp 0. An empty report shows the spec's new-user state instead of a lead.

*Rejected:* a server-generated lead. Every number is already in the report, and a client function with table-driven tests keeps the wording in one language layer.

### D13. Copy register on these pages

Every label on `/insights` and `/laporan` follows `ui-copy-register` (phase 3 D20):
- "Siklus gajian" → "Siklus gaji";
- "Ke mana uang siklus ini ngalir" → "Aliran uang periode ini";
- "Seberapa sering aku nanya" → "Seberapa sering ditanya";
- "Nunggu jawabanmu" → "Menunggu jawaban";
- "Udah …" → "Sudah …".

Formal "Anda" is rewritten impersonally, e.g. "Klik untuk mengatur batas anggaran" → "Atur batas anggaran". The phase 3 guard test's file list gains `components/analysis/*`, `components/report/*`, `components/insights/*`, `app/insights` and `app/laporan`. Server text such as category names and recurring charge labels is user data and stays.

### D14. Performance and bundle gates

Baselines are measured first, on the rebased branch, before any change.

- **`GET /dashboard/calendar`:** median of 20 calls under 150 ms, on a synthetic owner with 5 000 rows over 18 months, for a 31-day cycle.
- **`GET /dashboard/flow`:** under 200 ms; **`/flow/rows`:** under 100 ms for 50 rows.
- **Statement counts:** calendar ≤ 8, flow ≤ 10 and rows ≤ 4, constant in accounts and days (tested).
- **`/dashboard/analytics` and `/period-comparison`:** no more than 10% slower than the baseline with the additive fields.
- **`/insights` first-load JS:** no larger than the baseline. Removing `BurnCadenceChart` and `SpendingHeatmap` and loading Kalender and Peta Aliran dynamically pay for Ringkasan. The Kalender and Peta Aliran chunks are reported, and each must stay under 40 kB gzip.
- **`/laporan` first-load JS:** no more than +5 kB gzip.
- **Shared first-load JS (every route):** no more than +1 kB gzip.

### D15. Failure behaviour

- **Per-tab request fails:** `ErrorState` with "Coba lagi" inside that tab. The toolbar, the other tabs and the stepper keep working.
- **Calendar without an expected salary date:** the band says why ("Tanggal gaji berikutnya belum bisa diperkirakan: baru 2 gaji tercatat", using the phase 1 reason codes). `upcoming` runs to the window end and is titled "Sampai akhir periode".
- **Flow replay request fails:** the slider stays on the last shown day, an inline message says "Hari ini belum bisa dimuat", and play stops.
- **Flow rows fail:** the drawer shows `ErrorState`, and the link's amount stays visible.
- **An older API during a rolling deploy** answers 404 for the new routes. The tab shows "Belum tersedia, muat ulang sebentar lagi". The frontend and API ship in one release, so this lasts only for the deploy window.
- **`kakeibo` or `explanations` absent** (older API): the card falls back to "Data pembanding belum tersedia", and the figures render without their explanation. Rp 0 is never shown in place of a missing value.

### D16. Compatibility, migration and rollback

- **No migration.** The routes are new and the fields additive.
- **Unchanged:** `/insights` without `tab`, `/laporan?basis=&start=`, `lengkapi=1` and `atur=anggaran` keep their meaning.
- **Companion:** unaffected.
- **Rollback:** revert the pull request. No persistent state depends on this phase, apart from the per-viewer fold preference in `localStorage`, which an older build ignores.

### D17. Tests

- **Backend** (disposable Flyway-migrated PostgreSQL 16, `REQUIRE_TEST_DATABASE=1`):
  - calendar: days add up to Belanja with and without reimbursements, including a reimbursement in a later day; heat levels and thresholds; markers once per movement pair; top-ups apart from movements; waiting rows equal to the badge; plan statuses and the upcoming list with and without an expected date; month and cycle windows; owner isolation; 422; the statement count;
  - flow: conservation for every account node over a fixture with income, spending in all six buckets, excluded categories, reimbursements, transfers between accounts, parent ↔ pocket moves, goal pockets, top-ups, a sell back to cash, awaiting and ambiguous legs, a negative opening and a negative closing, an archived account, and `through` on every day of the window; summary figures equal to analytics for the full window; rows totals equal to links; 404 and 422; owner isolation; the statement count;
  - period comparison: `kakeibo.current` equals analytics `kakeibo` for the same window, and the existing fields keep their values (a fixture of today's response).
- **Frontend (Vitest):**
  - tabs and URL (unknown tab, `day`, `view`, the history writer);
  - the Kakeibo card (no percentages, unclassified call to action);
  - Belanja harian (peak, today, table alternative);
  - the calendar grid keyboard model and accessible names;
  - the week strip;
  - the Sankey layout module (deterministic positions for a fixture, no overlap, links ordered) and the map keyboard model;
  - the table completeness (every link once);
  - the fold and replay states, including reduced motion;
  - `reportLead` (table-driven: complete, incomplete, no comparison, unavailable figure, hidden balances);
  - Laporan's stepper and URL;
  - the copy guard over the new files;
  - `vitest-axe` on each tab and on Laporan.

## Preview conflicts (the approved rule wins)

| Preview | Rule | This change |
| --- | --- | --- |
| Kakeibo card compares raw pillar values from the prototype dataset | `observed-insights`: no percentage change | Amount changes ("Naik Rp X"), with no percentages |
| Kalender legend scale "Rp 0 → Rp 400rb+", fixed | Thresholds come from data (D6) | Per-period thresholds from the server, written in the legend |
| Calendar "Belum jelas" amounts are left out of day spending | Phase plan D9: waiting rows stay counted | Counted in `spend` where reportable, with the waiting marker and count |
| Peta Aliran "Belum jelas" is a separate destination for questions | D9: rows waiting for a category answer stay in their bucket | Only counterpart-waiting legs get their own node. Others are hatched inside their pillar link |
| "Keluar buat belanja" as one net number on the map | Offsets cannot balance a node | Belanja gross on the map, with Penggantian as a source; the net is in the summary figure |
| Heading "Ke mana uang siklus ini ngalir", caption "buat lihat transaksinya" | Copy register | "Aliran uang periode ini", "untuk melihat transaksinya" |
| "Kebiasaan dihafal" card on Ringkasan | Needs `GET /api/habits` (phase 5) | Not in this phase (OD42) |
| Laporan lead written casually | Copy register: reports are records (D12) | Neutral lead |

## Open decisions for owner

| ID | Question | Recommended default |
| --- | --- | --- |
| OD40 (resolved 2026-11-03: "approved, defaults ok") | Where category and budget management lives | **Stay on Analisis**, as Ringkasan's last card "Kategori & anggaran" (D5). Moving it to Pengaturan changes two specs and belongs with phase 5's plan |
| OD41 (resolved 2026-11-03: "approved, defaults ok") | What Ringkasan's Kakeibo compares | **Kakeibo buckets with net fresh saving for both windows** (D3), so the total equals Beranda's Keluar. The alternative, `pillars`, leaves savings transfers out |
| OD42 (resolved 2026-11-03: "approved, defaults ok") | "Kebiasaan dihafal" on Ringkasan | **Added by phase 5** together with `GET /api/habits`. Phase 4 leaves no placeholder |
| OD43 (resolved 2026-11-03: "approved, defaults ok") | The weekday heatmap card | **Remove it from Analisis.** Kalender shows spending per day. The API field `day_of_week_heatmap` stays for compatibility |
| OD44 (resolved 2026-11-03: "approved, defaults ok") | Account node granularity in Peta Aliran | **Top-level liquid accounts with their non-goal pockets folded.** Goal-linked pockets are saving nodes, and investment parents are investment nodes |
| OD45 (resolved 2026-11-03: "approved, defaults ok") | Replay speed | **One day per 800 ms after each response**, off under reduced motion, with the slider always available |
| OD46 (resolved 2026-11-03: "approved, defaults ok") | First day of the calendar week | **Monday**, as the preview and Indonesian calendars do |
| OD47 (resolved 2026-11-03: "approved, defaults ok") | Heat levels | **Five levels from the period's own quartiles of non-zero days**, rounded to Rp 1.000 and written in the legend. Fixed amounts would make most months one colour |
| OD48 (resolved 2026-11-03: "approved, defaults ok") | Expenses in excluded categories on the map | **A node "Tidak dihitung belanja"**, so every row on an account has a link and the account balances |

## Risks / Trade-offs

- **The flow fixture is the hardest part.** Conservation is proven per node on the fixture and on the seeded browser dataset. The node equation is also asserted against `get_accounts_with_balances` for `through = today`, so a missed row kind fails a test, not the owner's map.
- **The Sankey port is the largest frontend piece.** It is a deterministic module with its own tests. The table alternative is complete, so a layout defect never hides money.
- **Replay can create up to 31 requests in a cycle.** Each is cached and bounded by the statement count. Play waits for each response.
- **The numbers differ by design.** "Belanja" on the map is gross, while the Ringkasan and Transaksi Belanja are net. The map's summary figure and its explanation name the reimbursement source.
- **Overlap with other phases:**
  - Phase 3: `lib/urlState.ts`, `TransactionDrawer`, the copy guard and `globals.css`. This change rebases onto them after phase 3 merges.
  - Phase 5: the category section (OD40), `globals.css` alias removal and "Kebiasaan dihafal" (OD42).
  - Phase 6: Tanya intents may read `/dashboard/calendar` for "upcoming bills". The contract here is the one Tanya should call.
  - `docs/NEW_USER_GUIDE.md` and `lib/queryKeys.ts` are shared, append-only files.
- **Debt carried over:**
  - capture undo still leaves its `capture` feedback row (phase 2);
  - delete has no undo (phase 3, OD6).

## Migration Plan

None for the database. The release is the normal `deploy.yml` run after the owner merges. The frontend and the API ship together, and D15 covers the deploy window.
