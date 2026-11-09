## Why

On 2026-11-05 the owner chose design option 6 "Markas+" ("ok accepted") and expects the app to match it exactly. Laporan (`/laporan`) is still the pre-redesign page inside the new shell. The audit (`design-options/audit/AUDIT.md` §Laporan) measured four KPI tiles above two independent columns of eleven panels, 106 text pieces and 51 figures in the first screen, icon tiles, 11 type sizes and 8 weights, and a reading order nobody can follow. The original Laporan "read like a letter": four numbered Kakeibo questions, each with one figure and a short breakdown. On 2026-11-04 the owner said question 4 looked "half-built".

Known items for this phase:
- **320 px overflow.** One Laporan line runs past the frame at 320 px (shell verification), and the KPI values, the stepper and the pillar table have no room at that width (D1).
- **Hidden balances leak.** The table's change chips format digits with a fixed `id-ID` formatter, so they show amounts while balances are hidden; the history chart's accessible list does too (D20).
- **No way to write a first note.** The note panel appears only when this or the previous period already has a note, so a first note cannot be written (D13).
- **The untranslated-route registry.** `/laporan` leaves it. The untranslated-page test example stays on Pengaturan (`/settings`), where phase 6 moved it (D19, re-based).
- **One spending basis (owner decision 2026-11-08).** "Tempat belanja terbesar", the budget rows and their total, the reimbursement caption and the top-merchant fact use the Belanja basis, which counts expenses with the Tabungan pillar. Every "Pengeluaran" in the app must equal Beranda, Transaksi and Analisis for the same period, so Laporan reads additive Pengeluaran-basis fields (D29).

Phases 1 to 5 and 8, `add-category-system-keys`, `add-language-system`, `localize-server-text` and the credit flow are on `main` (`98cdb49`). Phase 6 (`apply-markas-analisis`, PR #95, `d79e95d`) is not merged yet; by the coordinator's instruction of 2026-11-09 this phase stacks on its branch (task 1.2). English is the default interface language, and every page rebuilt from phase 4 on ships English and Indonesian.

The phased plan is `cash-flow-tracker-prototypes/research/2026-11-05-markas-implementation-plan.md` (phase 7 of twelve). The fidelity source is `design-options/option-6/`: `styles.css` (it wins over `SPEC.md`), `js/pages2.js` lines 22–160 (Laporan), `SPEC.md` §6 to §9 and §13, and the captures `laporan-*` (Oktober, Juli, November running, and the salary cycle).

## Expected Outcome

- At 1440 px Laporan reads as in the prototype:
  - the title "Laporan" with "Laporan bulanan Kakeibo", the "Bulan kalender · Siklus gaji" segment and the stepper "‹ Oktober 2026 ›";
  - a reading article (728 px at 1440, at most 880 px) beside a 352 px context panel.
- The article opens with the one hero "Pengeluaran Oktober 2026" (Manrope 44) and one line "▲ Rp 753.830 vs September · ▲ Rp 1.050.497 vs rata-rata 3 bulan". Four numbered questions follow, 48 px apart with a rule between them:
  1. "Berapa uang masuk?": Masuk and the sources;
  2. "Berapa yang ditabung?": Ditabung & investasi with its share of Masuk and "Rincian tabungan";
  3. "Berapa pengeluarannya?": the pillar and category table with its total row, "Tempat belanja terbesar", the "Rincian pengeluaran" box, "Anggaran" and "Pengeluaran per bulan";
  4. "Apa yang mau diperbaiki bulan depan?": the facts with "Lihat transaksi", the saved note or "Tulis catatan untuk Oktober 2026", and last period's note when one exists.
- The context panel holds "Kualitas data" in one line with its fixes, "Selisih" ("masuk dikurangi pengeluaran") and "Laporan sebelumnya" with the current report marked by the straight selection bar.
- A running month shows progress only: "30% dari total Oktober 2026 · 37% dari rata-rata 3 bulan" with a bar, "Periode berjalan · dibandingkan setelah selesai", "berjalan" in the table and the chart, "Sejauh ini" for its facts, and no ▲, ▼ or verdict. Comparisons are always against whole periods.
- Every list row is one whole target that opens its ledger rows or its transaction. Merchant and unexpected amounts read "−Rp …" in the expense red; income sources "+Rp …" in green.
- "Pengeluaran per bulan" draws the last four completed periods and the running one as proportional bars with their amounts, the viewed period highlighted, the running one hatched, a legend and a table toggle.
- An English account sees Laporan in plain English ("Report", "Spending", "How much came in?"). An Indonesian account sees neutral, firm Indonesian. Laporan no longer shows "This page is in Indonesian for now."
- Every money figure, count and date on Laporan equals today's value for the same data, in both languages.

## Approval

Approved under the owner's standing instruction "complete all until finished" (2026-11-06, renewed 2026-11-08 and with "continue all" on 2026-11-09); decisions take their recommended defaults. The coordinator accepted the three flagged defaults (D11, D13, D26) under that instruction on 2026-11-09. The owner's decision of 2026-11-08, "one spending basis", applies to Laporan as D29.

Every decision, its default and the rejected alternatives are in `design.md` (D1 to D28, summarized in "Decisions and their defaults"). Three decisions are flagged and are reviewed before implementation:
- **D11 (scope):** "Pengeluaran per bulan" becomes one series of bars, as the prototype draws it. Today each month's bar is split into its five pillars without a legend. The per-month pillar split leaves the page; each month's pillars stay in that month's table. Fallback: keep the split with a pillar legend and table columns per pillar.
- **D13 (scope):** the note can be written for the first time. Question 4 shows the saved note read-only with "Ubah", or a quiet "Tulis catatan untuk {period}" that opens the form in place, and never an empty form on load. Today the block is hidden without a note, which leaves no way to write the first one. Fallback: the prototype's always-visible form.
- **D26 (scope):** "Pengeluaran pakai kredit" and "Tagihan kredit dibayar" are not shown on Laporan. `add-credit-spending-flow` hands Laporan nothing (its blocks are H5, H8, H9 and H11), and the report API has neither figure for a closed period. Fallback: a separate backend change adds both to the report, and a later UI change shows them under question 3.

No decision changes money behaviour beyond the owner's one-basis decision (D29): every figure keeps its server source, every action sends today's request, and the backend only adds read-only Pengeluaran-basis fields beside the existing ones, whose values do not change.

## Deviations from the prototype

Each is a decision in `design.md` and is captured for the owner's end review:
1. The stepper keeps phase 5's reset "Bulan ini" / "Siklus ini" (D2).
2. **Owner rule:** the hero line compares with both the whole previous period and the 3-month average (D3). The prototype names only the previous month.
3. The running hero line also gives the shares against the previous period and the average, with a bar (D3). The prototype says only "Periode berjalan · dibandingkan setelah selesai".
4. Income sources, merchants, budget rows and facts are whole-row links to their ledger rows; the table's rows are whole-row links too (D5, D7, D8, D10, D12). The prototype's rows are plain text.
5. "Rincian tabungan" keeps today's two rows (saving-pillar expenses, net moves into savings and investments) instead of one row per destination, which the API does not provide (D6).
6. The table keeps "belum lengkap" against a partial previous period and the per-pillar shares while running (D7).
7. "Biaya hidup pokok · saat ini" keeps the server's method sentence and drops the emergency-fund months, which Target & Tagihan shows (D9).
8. A budget row reads the amounts, "Rp 620.000 dari Rp 500.000 · lebih Rp 120.000", besides the bar (D10). The prototype gives a percent only.
9. "Atur anggaran" opens the shared budget dialog of phase 6 (D10). The prototype links to Pengaturan, which holds only the overall limit.
10. The month bars are proportional inside a 112 px bar area, fixing the prototype's equal heights, and have a legend and a table (D11).
11. The facts are the server's deterministic facts (`monthly-report` "Improvement Facts"), not the prototype's three client-made sentences (D12).
12. The previous period's note line appears only when that note exists (D13). The prototype writes "belum ada".
13. "Selisih" is shown for completed periods only (D14). The prototype shows it for the running month too.
14. The classification dialog ("Lengkapi klasifikasi"), which the prototype does not draw, uses the SPEC dialog parts at 560 px (D15).

## What Changes

- **Title block:** subtitle; `Segmented` "Bulan kalender · Siklus gaji" (cycle only when available); the shared `PeriodStepper` with the report's dates (D2).
- **Layout:** `.cols` with the article (`max-width: 880px`) and the 352 px panel from 1200 px; one column below; phone below 768 px, by viewport media queries (D1).
- **Hero:** `HeroFigure` without a mark (phase 4), the line with `ChangeFigure` (phase 6) against the previous period and the average, or the running shares with `Meter` (phase 6) (D3).
- **Questions:** `.q` sections (32 px number column, gap 16, 48 px apart with a 1 px rule; stacked on the phone) (D4), with:
  - Q1 the Masuk figure and the source rows (D5);
  - Q2 the Ditabung & investasi figure with "{n}% dari uang masuk" and "Rincian tabungan" (D6);
  - Q3 the `.tbl` pillar and category table with its total row (D7), "Tempat belanja terbesar" (D8), the "Rincian pengeluaran" disclosure box (D9), "Anggaran" with `Meter` rows (D10) and "Pengeluaran per bulan" with `ChartTable` (phase 6) (D11);
  - Q4 the facts and the note (D12, D13).
- **Context panel:** Kualitas data, Selisih, Laporan sebelumnya (D14).
- **Dialogs:** the classification dialog on `ShellDialog` 560 with the same requests; the budget dialog from phase 6 (D10, D15).
- **Language:** a `report` namespace; facts and period labels from the server's codes (`localize-server-text`); `/laporan` added to the translated routes and removed from `untranslatedPages.ts`; the untranslated-page test example stays on Pengaturan (`/settings`) (D19).
- **Backend (additive, read-only, D29):** the report gains `spending.top_spending_merchants`, `budget.categories[].spending` and `spending_remaining`, `budget.total_spending`, `budget.overall.spending` and `spending_remaining`, and `flow.spending_reimbursement_offset`; the comparison gains `spending_top_merchant`. All reuse `services/spending_basis.py`; no existing field, request, schema or migration changes.
- **Removed** (each with its destination in D22):
  - the KPI strip (`KpiStrip`, `Kpi` and their CSS, whose last user is Laporan);
  - `ReportLayout` and the two columns, `ReportPanel`, `AmountRow`, `CardHeader` icon tiles on Laporan;
  - `ComparisonParts.tsx` (`Change`, `Progress`, `PillarTable`, `ImprovementFacts`, `MonthlyHistory`), replaced by the shared pieces and the new question sections;
  - `periodNote`'s info notice, the green "Data lengkap" notice and `report.module.css`;
  - Laporan's use of `useWidthAtMost`.
- **BREAKING (spec):** `monthly-report` "Report page layout", "Four Kakeibo questions in a fixed order", "Running Period Progress" and "Monthly Spending History" change their presentation.

## Non-goals

- No change to any existing API field, request, figure, count, money rule, schema, data or the Android companion. The backend only adds the read-only Pengeluaran-basis fields of D29; `db/` and `mobile/` are untouched, and there is no migration.
- No change to what a report compares, which periods are listed, the readiness rule, the nav badge, the bell item, the notes' storage or the drill-down filters.
- No credit figures (D26) and no capital gain, which the report has no field for (D27).
- No restyle of other pages. Rekening is redesigned (phase 8); Target & Tagihan is phase 9.
- No new npm dependency.
- Local verification only, with the parity seed, the audit seed and the stress and USD seed in disposable databases. Release and production stay with the owner through the approved GitHub workflow.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `monthly-report`:
  - "Report page layout": the article, questions, hero and context panel;
  - "Four Kakeibo questions in a fixed order": the numbered questions and the note entry;
  - "Running Period Progress": the hero line with both shares;
  - "Monthly Spending History": one series with a legend and a table.
  - Added: "Laporan in Both Languages".
- `monthly-report` (D29), added: "One Pengeluaran on Laporan".
- `monthly-navigation-stepper`: the report's period name in both languages; the untranslated-page example stays Pengaturan.
- `tactile-pulse-ui`: "No Decorative Effects" names Target & Tagihan as the page not yet redesigned, and adds a Laporan scenario.

`interface-language` and `ui-copy-register` need no delta. Adding `/laporan` to the translated routes follows their rule that the set of untranslated pages only shrinks. `report-periods-and-drilldown` and `financial-reporting-consistency` keep their requirements: the links and figures do not change.

Standards:
- UI and accessibility (WCAG 2.2 AA): landmarks and headings (one `h1`, questions as `h2`), the table with its caption, row headers and total row, the disclosure box, focus return from every dialog, target size, reflow at 320 px and 400 % zoom, non-text contrast of bars and meters, non-colour cues on changes (▲/▼ with words), and 3.1.1 and 3.1.2 language of page and parts;
- the copy register in both languages (`ui-copy-register`);
- `money`: figures identical before and after, and in English and Indonesian;
- `personal-data`: the note is shown only to its owner and never logged.

No capability in `AGENTS.md` changes.

## Impact

- **Frontend Laporan:**
  - `app/laporan/page.tsx` (split into the page, `ReportHero`, `ReportQuestions` and `ReportContext`) and `page.test.tsx`;
  - `components/report/`: new `IncomeQuestion.tsx`, `SavingQuestion.tsx`, `SpendingQuestion.tsx` (table, merchants, details, budget, history), `ImprovementQuestion.tsx` (facts, note), `ReportContext.tsx`; `ClassificationDialog.tsx` onto `ShellDialog`; `DataQualityPanel.tsx` into the panel line; `ReflectionAnswer.tsx` into `ImprovementQuestion.tsx`; `BudgetPanel.tsx` (rows; the dialog already moved in phase 6);
  - removed: `ComparisonParts.tsx`, `ReportLayout.tsx`, `ReportParts.tsx`, `SpendingAnswer.tsx`, `IncomeSavingsAnswers.tsx`, `report.module.css`;
  - `lib/monthlyReport.ts` (types unchanged; link helpers kept).
- **Shared:** `components/workspace/primitives.tsx` (`Kpi`, `KpiStrip` removed), `app/globals.css` (`.ws-kpis`, `.ws-kpi*` removed; `.q`, `.q-num`, `.q-h`, `.bars-v`, `.b`, `.box` disclosure rules added from `styles.css`).
- **Language:** `messages/en.json` and `id.json` (`report`), `lib/i18n/translatedRoutes.ts` (`/laporan`), `components/workspace/untranslatedPages.ts` (the `/laporan` entry removed), the copy register roots; the untranslated-page test keeps its example on Pengaturan (`/settings`).
- **Tooling (local only):** `scripts/verify/markas/`: the Laporan scenarios, `laporan-checklist.js`, the keyboard flows `laporan-keys.mjs`, the parity seed extended with five months of history, a partial first month, a reflection note on the previous period, a budget over its limit and the running month.
- **Backend (D29):** `services/spending_basis.py` (`reimbursed`, `spending_merchants`), `services/observed_insights.py` (`merchant_spending` takes an expense filter), `services/monthly_report.py` and `services/monthly_comparison.py` (the additive fields), with `tests/test_report_spending_basis.py`.
- **No change** to `db/`, `mobile/` or `nginx/`. Rollback is a revert of the merge commit.
- **Dependencies:** phase 6 (`apply-markas-analisis`) for `Meter`, `ChangeFigure`, `ChartTable`, `format.compactMoney`, `BudgetDialog` and `progress.baseline_share`; phase 5 for `RowAmount`, `TransactionDialog` and the stepper; phase 4 for `HeroFigure` without a mark.
