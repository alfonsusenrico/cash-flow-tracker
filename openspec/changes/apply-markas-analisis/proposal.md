## Why

On 2026-11-05 the owner chose design option 6 "Markas+" ("ok accepted") and expects the app to match it exactly. Analisis (`/insights`) is still the pre-redesign page inside the new shell. The audit (`design-options/audit/AUDIT.md` §Analisis) measured 14 type sizes, 9 weights, 62 left edges, 102 fold text items, 15 long captions, icon tiles on card titles, the same three figures as Beranda in a KPI row, and two system cards ("Seberapa sering ditanya", "Kebiasaan dihafal") that are not about money.

Five known items belong to this phase:
- **Kalender marker contrast.** axe reports `color-contrast` on the calendar markers at 1024 px and below (shell verification, `apply-markas-shell` 9.1). The markers are text pills on heat fills (`--heat-1..5`); the selected-day bar measures 1.58–1.95:1 on the two hottest light fills (D11).
- **Peta Aliran labels.** Four node labels run past the frame at every desktop width, because labels have no gutter and the SVG scales with its host (D15).
- **The stepper label stays Indonesian in English.** Analisis passes the server's `timeframe_label` to the shared stepper without the period dates, so an English account reads "Previous month · Oktober 2026 · Next month" (`add-language-system` verification, D2).
- **320 px overflow.** The KPI values, the daily chart's 76 px axis column and the flow table exceed a 288 px column (D1, D26).
- **The dotted explain figure.** The Pengeluaran KPI is still an `ExplainableFigure` with a dotted underline, which `explainable-figures` allows only on pages not yet redesigned (D4).

Phases 1 to 5 (foundation, shell, Beranda, Tinjau, Transaksi), `add-category-system-keys`, `add-language-system` and `localize-server-text` merge before this phase. English is the default interface language, and every page rebuilt from phase 4 on ships English and Indonesian.

The phased plan is `cash-flow-tracker-prototypes/research/2026-11-05-markas-implementation-plan.md` (phase 6 of twelve). The fidelity source is `design-options/option-6/`: `styles.css` (it wins over `SPEC.md`), `js/pages.js` lines 490–805 (Analisis), `js/dialogs.js` lines 580–588 (`kategori`), `SPEC.md` §6 to §9 and §13, and the captures `analisis-*` and `dlg-kategori-*`.

## Expected Outcome

- At 1440 px Analisis reads as in the prototype:
  - the title "Analisis" with its subtitle, the "Siklus gaji · Bulan kalender" segment and the period stepper "‹ Siklus 23 Okt – 24 Nov ›";
  - underline tabs Ringkasan · Kalender · Peta Aliran with "Baca Laporan Bulanan" at their right;
  - on Ringkasan, a 728 px reading column beside a 352 px context panel.
- **Ringkasan** shows one 44 px hero "Pengeluaran · siklus 23 Okt – 24 Nov sejauh ini" with one quiet line "Masuk … · Ditabung & investasi …" and one "Rincian". Below it:
  - two whole-period comparison rows, "64% dari total siklus 25 Sep – 22 Okt" and "65% dari rata-rata 3 bulan (Agu–Okt)", each with the amount and a bar;
  - "Kakeibo · per pilar" with amount, share and the previous period;
  - "Pengeluaran harian" as bars with a table toggle;
  - "Pengeluaran per kategori" grouped by pillar under 2 px rails, with budget bars, "Kelola kategori" and "Atur anggaran".
  The panel holds "Tagihan & langganan" and the "Laporan bulanan" link.
- A completed period shows the same rows as changes, "▲ Rp 753.830 dibanding total siklus …" and "▼ Rp 120.000 dibanding rata-rata 3 bulan", never a same-date comparison. A running period shows progress only, with no ▲, ▼ or verdict.
- **Kalender** shows the hero "Pengeluaran periode ini" with "Hari ke-14 dari 33 · 19 hari lagi sampai gaji 24 Nov", a Monday-first grid of cells at least 80 px high (56 px on the phone) with compact amounts ("79rb"), 6 px markers, a 4 px heat strip and a selected day with a 2 px top bar. The day panel lists the day's transactions and "Sampai akhir periode". Arrow keys, Home, End, PageUp and PageDown move the day.
- **Peta Aliran** shows the hero "Masuk" with four supporting figures, the "Sorot aliran" and "Peta · Tabel" segments, and a four-column map:
  - bands at 0.30 opacity, or 0.06 when dimmed;
  - every label inside the frame;
  - a legend.
  Every link is in the table.
- Expense amounts in the day panel and the flow detail rows read "−Rp …" in the expense red through the shared amount component of phase 5.
- "Seberapa sering ditanya" moves to Pengaturan › Kebiasaan in this same change, so no release lacks it. "Kebiasaan dihafal" is removed, because Pengaturan › Kebiasaan already lists every habit. Category management moves from the page into the "Kelola kategori" dialog.
- An English account sees Analisis in plain English ("Analysis", "Spending", "Daily spending", "Money flow"), with the stepper reading "Cycle 23 Oct – 24 Nov". An Indonesian account sees neutral, firm Indonesian ("Pengeluaran", never "Belanja" as a figure label). Analisis no longer shows "This page is in Indonesian for now."
- Every money figure, count and date on Analisis equals today's value for the same data, in both languages. The only new figure is the share of the running period's Pengeluaran in the 3-month average, computed by the server (D5).

## Approval

Approved under the owner's standing instruction "complete all until finished" (2026-11-06, renewed 2026-11-08); decisions take their recommended defaults.

Every decision, its default and the rejected alternatives are in `design.md` (D1 to D30, summarized in "Decisions and their defaults"). Four decisions are flagged and are reviewed before implementation:
- **D5 (backend addition, read-only; money presentation):** `GET /reports/monthly/comparison` gains `progress.baseline_share`, the running period's Pengeluaran so far as a whole percent of the baseline's Pengeluaran, rounded half up as `progress.share` is. No existing field, figure or rule changes. Fallback if rejected: the second row shows the average's amount "per bulan" without a percent and without a bar.
- **D6 (scope):** while the running period is shown, the per-pillar comparison of the last completed period with its average leaves the first screen. It appears when the owner steps back to that period, where each pillar row shows its change against the 3-month average. Fallback: keep a second block "Periode lalu vs rata-rata 3 bulan" under Kakeibo while running.
- **D15 (scope):** the fold control "Tampilkan uang masuk, saldo dan Masih ada" is removed. Income is always drawn, and the opening and "Masih ada" links are never drawn on the map: "Masih ada" is a supporting figure, and the table lists every link. Fallback: keep the control, default folded.
- **D29 (scope):** "Pengeluaran pakai kredit" and "Tagihan kredit dibayar" are not shown on Analisis. `add-credit-spending-flow` hands Analisis nothing (its hand-off blocks are H5, H8, H9 and H11), and the figures exist only for the current cycle and per credit account, so a period view needs a new server field. Fallback: a separate backend change adds both figures to the analytics window, and a later UI change shows them in the Pengeluaran Rincian.

## Deviations from the prototype

Each is a decision in `design.md` and is captured for the owner's end review:
1. The stepper keeps phase 5's label and reset ("Siklus 23 Okt – 24 Nov", "Siklus ini"). The prototype reads "23 Okt – 24 Nov · berjalan" and has no reset; "sejauh ini" in the hero says the period is running (D2).
2. The tabs stay an ARIA tab list with `?tab=` in the URL (`analysis-workspace`). The prototype uses links to three hash routes (D3).
3. "Baca Laporan Bulanan" opens the latest closed report (`/laporan`), not a fixed month (D3).
4. The hero keeps one "Rincian" for the Pengeluaran lines the server sends. The prototype hero has none (D4).
5. A completed period's comparison rows read as changes with ▲ or ▼ (D5). The prototype draws only the running period.
6. The Kakeibo rows are whole-row links to the ledger, and "Belum diklasifikasi" keeps its "Perlu dipilih" fix next to the row (D6).
7. The daily chart draws its width at the column's real size instead of scaling a 720 px drawing, omits the "0" tick, never prints two date labels on top of each other, and gives today's hatch a legend swatch (D7).
8. "Tanpa kategori" and archived categories with spending stay in the category list, under "Belum diklasifikasi" and their pillar (D8).
9. "Atur anggaran" opens the shared budget dialog (D9). The prototype links to Pengaturan, which holds only the overall limit.
10. "Kelola kategori" edits and adds categories inside the same dialog, and lists archived categories with "Pulihkan" (D9). The prototype's buttons only show toasts.
11. The bills panel's second button reads "Lihat semua" and opens the list of detected charges, because a page has at most one "Rincian" (D10).
12. The calendar strip's opacity comes from the server's heat level 1–5, and its legend names the lowest and highest thresholds (D11).
13. The phone shows the whole calendar grid instead of the app's week strip, as the prototype does (D13).
14. The flow map keeps link patterns for transfers, investments and waiting links and adds a legend, so colour is never the only cue (D15).
15. The flow table keeps its "Transaksi" column from 768 px (D16).
16. Phone segments and small buttons are 40 px high instead of 26 or 28 px (D1).

## What Changes

- **Title block:** subtitle; the shared `Segmented` "Siklus gaji · Bulan kalender"; the shared `PeriodStepper` with the selected period's dates, so its label follows the interface language (D2).
- **Tabs:** underline tabs (`.tabs`, `.tab`) as an ARIA tab list with arrow, Home and End keys; "Baca Laporan Bulanan" at their right from 768 px (D3).
- **Ringkasan:**
  - `HeroFigure` without a mark: Pengeluaran, the quiet line Masuk · Ditabung & investasi, and one "Rincian" built on `ExplanationDialog` (D4);
  - two comparison rows (`.cmp-row`) with the shared `Meter` bar: progress while running, ▲/▼ changes when complete (D5);
  - Kakeibo per pilar rows with the change against the average when complete (D6);
  - Pengeluaran harian chart with the shared chart-and-table toggle (D7);
  - Pengeluaran per kategori in pillar groups with 2 px rails, budget bars and the zero toggle (D8);
  - "Kelola kategori" and "Atur anggaran" dialogs (D9);
  - the context panel: Tagihan & langganan and Laporan bulanan (D10).
- **Kalender:** hero and the days-left line, the grid with compact amounts, 6 × 6 markers, the 4 px heat strip, the day panel with the shared transaction dialog, "Sampai akhir periode", and the full grid on the phone (D11 to D13).
- **Peta Aliran:** hero Masuk with four supporting figures, "Sorot aliran", "Peta · Tabel", the restyled four-column map with its labels inside the frame and a legend, the table, and the node and link details in a centered dialog (D14 to D16).
- **Backend, additive and read-only (flagged, D5):** `progress.baseline_share` in the monthly comparison.
- **Moved:** "Seberapa sering ditanya" to Pengaturan › Kebiasaan, unchanged in behaviour (D18).
- **Shared pieces built here and reused by phase 7 (D24):** `Meter` (the 4 px `.prog` bar), `ChangeFigure` (▲/▼ with the words for screen readers, through the money formatter), `ChartTable` (the chart/table toggle), `format.compactMoney` ("79rb", "1,3jt"; "79K", "1.3M"), and the category `BudgetDialog`.
- **Language:** an `analysis` namespace; Kakeibo names from `terms`; dates and compact amounts through `lib/format.ts`; `/insights` added to the translated routes and removed from `untranslatedPages.ts`; the untranslated-page test example moved from Analisis to Laporan (`/laporan`) (D22).
- **Removed** (each with its destination in D25):
  - the KPI row (`KpiStrip` use on Analisis), `ProgressCard`, `BaselineCard` and the dotted `ExplainableFigure` use on Analisis;
  - `HabitsLearnedCard` and its test;
  - `CategoryBudgetSection` (into the two dialogs);
  - `ObservedInsights.tsx` (into the bills panel) and `components/insights/` (QuestionRate moves to `components/settings`);
  - the replay ("Putar ulang", slider, `REPLAY_MS`), the fold control, `MiniMap`, the undefined `mapWide` class and the `Drawer` use of Peta Aliran;
  - the week strip and `ProgressBand`, the heat levels' fills and the `--heat-1..5` tokens with their test pairs;
  - Analisis's use of `ReportLayout`, `ComparisonParts` and `useWidthAtMost`;
  - the dead `components/charts/theme.ts`;
  - the in-page rolling-period caption, replaced by a line under the title only when the shared period is rolling.
- **BREAKING (spec):** `analysis-workspace` "Ringkasan Supporting Cards" is replaced by "Ringkasan Context Panel" and "Spending by Category on Ringkasan"; `cycle-calendar` "Kalender Phone Week Strip" by "Kalender on the Phone"; `money-flow-map` "Peta Aliran Replay" is removed and "Peta Aliran Fold" is replaced by "Peta Aliran Balances". The flow endpoint keeps `through` for API consumers.

## Non-goals

- No change to any existing API field, request, figure, count, money rule, schema, data or the Android companion. The only backend work is the additive, read-only `progress.baseline_share` (D5). `db/` and `mobile/` are untouched, and there is no migration.
- No change to what a comparison compares: whole previous period and the average of up to three whole, non-partial periods, as `monthly-report` defines. No same-date comparison returns.
- No credit figures on Analisis (D29) and no capital gain, which Analisis has no field for (D30).
- No restyle of Pengaturan › Kebiasaan beyond adding the moved panel; phase 10 rebuilds it.
- No Laporan body; phase 7 owns it. Laporan only switches the import path of the budget dialog here.
- No new npm dependency.
- Local verification only, with the parity seed, the audit seed and the stress and USD seed in disposable databases. Release and production stay with the owner through the approved GitHub workflow.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `analysis-workspace`: tabs, period basis, Ringkasan figures, comparison, daily spending, layout and accessibility. Removed: "Ringkasan Supporting Cards". Added: "Ringkasan Context Panel", "Spending by Category on Ringkasan" and "Analisis in Both Languages". The states stay in "Analisis Layout and Accessibility".
- `cycle-calendar`: heat levels, grid and day sheet as the prototype draws them. Removed: "Kalender Phone Week Strip". Added: "Kalender on the Phone".
- `money-flow-map`: map and keyboard, table and centered details. Removed: "Peta Aliran Replay" and "Peta Aliran Fold". Added: "Peta Aliran Figures" and "Peta Aliran Balances".
- `monthly-report`: "Whole-Period Comparison API" gains the additive `progress.baseline_share` (D5).
- `habit-learning`: "Seberapa Sering Ditanya Panel" moves to Pengaturan › Kebiasaan.
- `finance-dashboard-cockpit`: "Inline Category Budget Editing" becomes the "Atur anggaran" dialog.
- `custom-category-management`: "Category Budget Variance Filtering" describes the category list and the "Kelola kategori" dialog.
- `report-periods-and-drilldown`: "Non-Editable Insight Rows" in the dialog.
- `indonesian-daily-finance-ui`: "Intuitive Daily Finance Terminology" names Analisis's new labels.
- `monthly-navigation-stepper`: the untranslated-page example moves from Analisis to Laporan.
- `tactile-pulse-ui`: "No Decorative Effects" names Laporan and Rekening as the pages not yet redesigned, and adds an Analisis scenario.

`interface-language` and `ui-copy-register` (from `add-language-system`) need no delta. Adding `/insights` to the translated routes follows their rule that the set of untranslated pages only shrinks.

Standards:
- UI and accessibility (WCAG 2.2 AA): landmarks and headings, the tab list pattern, the calendar grid pattern, focus return from every dialog, focus not obscured by the sticky top bar, target size, reflow at 320 px and 400 % zoom, non-text contrast of markers, bars and bands (1.4.11), non-colour cues (1.4.1), reduced motion, and 3.1.1 and 3.1.2 language of page and parts;
- the copy register in both languages (`ui-copy-register`);
- `money`: figures identical before and after, and in English and Indonesian; the new share proven against the comparison's own amounts;
- `api-consumers`: one additive field, announced in the delta; the companion does not read it;
- `database`: no migration; the backend suite runs on migrated disposable PostgreSQL because the backend changes;
- `personal-data`: no rendered text or user data in logs.

No capability in `AGENTS.md` changes.

## Impact

- **Frontend Analisis:**
  - `app/insights/page.tsx` and `page.test.tsx`;
  - `components/analysis/`: `RingkasanTab.tsx` (rebuilt), new `AnalysisTabs.tsx`, `ComparisonRows.tsx`, `KakeiboRows.tsx`, `DailySpendingChart.tsx`, `CategorySpending.tsx`, `BillsPanel.tsx`, `ReportLink.tsx`; `calendar/KalenderTab.tsx` (rebuilt); `flow/PetaAliranTab.tsx` and `flow/sankey.ts` (restyled layout options, replay and fold removed); `format.ts` (formatters to `lib/format.ts`), `types.ts`, `url.ts`, `analysis.module.css` (Analisis-only rules), `analysis.test.tsx`;
  - `components/analysis/CategoryBudgetSection.tsx` and `HabitsLearnedCard.tsx` removed; `components/insights/` removed (`ObservedInsights.tsx` into `BillsPanel.tsx`, `QuestionRate.tsx` to `components/settings/`);
  - new `components/categories/CategoryManagerDialog.tsx`, `CategoryForm.tsx` and `BudgetDialog.tsx` (the last moved from `components/report/BudgetPanel.tsx`).
- **Shared:**
  - `components/workspace/figures.tsx` (`Meter`, `ChangeFigure`), new `components/charts/ChartTable.tsx`, `lib/format.ts` (`compactMoney`);
  - `app/globals.css`: `.tabs`, `.tab`, `.cmp-row`, `.prog`, `.grp`, `.grp-h`, `.cal`, `.cal-cell`, `.heat`, `.marks`, `.chart`, `.chart-foot`, `.ax`, `.flow`, `.lbl-node`, `.lbl-amt`, `.supports` at `--n: 4`, `.tbl` and `.more-row` from `styles.css`;
  - `frontend/tokens.css` and `app/tokens.test.ts` (the `--heat-*` tokens and pairs removed);
  - `components/charts/theme.ts` removed;
  - `components/report/BudgetPanel.tsx` and `ComparisonParts.tsx` (imports only: the moved dialog and `format.compactMoney`).
- **Pengaturan:** `components/settings/HabitsSettings.tsx` gains the moved "Seberapa sering ditanya" panel; its test absorbs `QuestionRate.test.tsx`.
- **Language:** `messages/en.json` and `id.json` (`analysis`, `terms` additions, `errors` codes if any), `lib/i18n/translatedRoutes.ts` (`/insights`), `components/workspace/untranslatedPages.ts` (the `/insights` entry removed), the copy register roots, and the untranslated-page test of `add-language-system`, which moves its example from Analisis to Laporan.
- **Backend (additive, read-only, flagged D5):** `services/monthly_comparison.py` (`progress.baseline_share`) and a test in `tests/test_monthly_comparison.py`.
- **Tooling (local only):** `scripts/verify/markas/`: the Analisis scenarios, `analisis-checklist.js`, the keyboard flows `analisis-keys.mjs`, the parity seed extended with a 33-day running cycle, a partial month and an archived category with spending, and the stress and USD seed for Analisis.
- **No change** to `db/`, `mobile/` or `nginx/`. Rollback is a revert of the merge commit; the backend addition is read-only.
- **Follow-up dependency:** phase 7 (`apply-markas-laporan`) reuses `Meter`, `ChangeFigure`, `ChartTable`, `compactMoney` and `BudgetDialog`, and needs `?lengkapi=1` on Laporan to keep opening the classification dialog, which "Perlu dipilih" uses.
