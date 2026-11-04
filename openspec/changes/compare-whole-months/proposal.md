# Proposal

## Why

The owner wants to understand their monthly money habits and find what to fix. Today Analisis and Laporan compare a period with the same number of elapsed days of the previous one, and Laporan uses a "three-period median" over the same first days. The owner rejected that:

> "I don't think comparing date to date is good because monthly money management is very volatile and dynamic, not always at every monday or every date 10 I bought my needs right?"

They want three things:
- each month's spending categorised, sorted and cleanly organised;
- a completed month compared with the previous month and with their usual months;
- facts that point to what to improve.

## Expected Outcome

On Laporan and Analisis, a completed month shows:
- Pengeluaran, Masuk, Ditabung & investasi and Selisih;
- each with ▲ or ▼ against the previous whole month and against the average of the three completed months before it (Oktober against Jul–Sep);
- spending grouped by pillar with categories sorted by amount, their share and change;
- "Yang bisa diperbaiki": short facts with figures and links to their transactions;
- a chart of the last months.

A running month shows only its progress against the previous month's total, such as "30% dari total Oktober 2026", and never "naik" or "turun". No figure compares the same date across periods.

## Approval

1. **Owner logic, 2026-11-04.** The owner described how they want to use the app:
   - "I want to know how much of my expense per month, categorized, sorted, and organized cleanly so I can see which one is expense, which one is wants, needs, savings";
   - "I want to know if there is improvement or degradation of my monthly expense, not dod not wow not mom. It's simply compared to previous month or months how's my money management";
   - "based on that reports, hopefully I can understand my own behavior, finding places to fix, improve my money management better".

   These expectations are binding. They are recorded in the agent's memory note `owner-money-logic-expectations`.
2. **The 3-month window, 2026-11-04.** After the agent explained the average with an example, the owner said: "ok use that window, approved, create the plan". The window is the three completed months before the viewed month. Oktober uses Jul–Sep, and November, once finished, uses Agu–Okt. The month being judged is never in its own average.
3. **Final preview, 2026-11-04.** The Laporan and Analisis Ringkasan screens of `cash-flow-tracker-prototypes/2026-11-04-workspace-refinement/preview.html` and `compare.html` are the visual target. The same approval also covers the recommendations the owner accepted ("other than that can use your recommendation"):
   - "Pengeluaran" without savings;
   - no Laporan prose lead;
   - "Kualitas data" as one line;
   - the reflection block hidden.
4. **Still binding from the redesign phases:**
   - `ui-copy-register` and the "Spending Terms" of `refine-workspace-layout`;
   - "Report equals the ledger";
   - D13 of phase 4: the client computes no money value;
   - WCAG 2.2 AA verification.
5. **One question is still open:** OD88, whether the running month gets a facts-only "Yang bisa diperbaiki". It has a recommended default in design.md.

**Plan approval, 2026-11-04.** After a summary of both refinement plans (`refine-workspace-layout` and `compare-whole-months`), the owner said "approved, defaults ok". OD88–OD95 take their recommended defaults (design.md, open decisions).

## What Changes

- **Comparison logic:**
  - whole periods replace same-elapsed-day windows on Laporan and Analisis, for calendar months and salary cycles;
  - a completed period is compared with the previous whole period and with the average of up to three completed full periods before it;
  - a running period shows only progress against the previous period's total.
- **New read-only `GET /api/reports/monthly/comparison`:**
  - period totals (Pengeluaran, Masuk, Ditabung & investasi, Selisih);
  - the previous period;
  - the baseline average;
  - running progress;
  - pillars with their categories;
  - improvement facts;
  - the history of the last four completed periods plus the running one.

  The same object replaces `comparison` in `GET /api/reports/monthly`.
- **Laporan:**
  - KPI-led;
  - "Kualitas data" as one line;
  - the pillar and category table;
  - "Yang bisa diperbaiki";
  - the monthly chart;
  - top merchants;
  - no written lead;
  - the reflection block shown only when a note exists.
- **Analisis Ringkasan:**
  - the running period's progress against the previous period, in total and per pillar;
  - "{last completed month} vs rata-rata 3 bulan";
  - compact cards for bills, how often the app asked, and learned habits, with details behind "Rincian";
  - the same-day Kakeibo marks removed.
- **`GET /api/dashboard/period-comparison`:** stays unchanged for API consumers but is no longer shown by the web. It is deprecated, and its removal is a later change.

## Non-goals

- Beranda, Transaksi, Rekening, Target & Tagihan, the palette and navigation. These belong to `refine-workspace-layout`, which ships first.
- Budgets, forecasts and verdict words such as "boros" or "hemat".
- Any AI-written insight. Every fact is computed by deterministic code from ledger rows.
- Changes to the companion app. It reads only `GET /api/reports/monthly/latest`, which is unchanged.
- A database migration.
- **Production:** any action outside the merge and `deploy.yml`. Local verification never grants production authority; the post-deploy check is read-only.

## Capabilities

**New capabilities:** none.

**Modified capabilities:**
- `monthly-report`:
  - whole-period comparison and a three-period average;
  - running progress;
  - pillar and category breakdown;
  - improvement facts and history;
  - the comparison API;
  - a KPI-led page with no written lead;
  - the reflection block hidden unless a note exists.
- `observed-insights`: the fair comparison stays an API figure that the web does not present.
- `analysis-workspace`: Ringkasan figures, its comparison and its supporting cards.

**Project capabilities:** unchanged. The change touches `money` (read-only figures from existing rules), `multi-user` (owner-scoped), `api-consumers` (an additive endpoint; the report's `comparison` object changes shape, and only the web reads it) and `deployed`.


## Ordering

This change ships second, on `feat/whole-month-comparisons`. It rebases onto `main` after `refine-workspace-layout` merges, because it uses that change's spending terms and its additive `spending` fields. Two pull requests keep the visual correction and the logic change reviewable and reversible on their own.

## Impact

- **Backend:**
  - new `services/monthly_comparison.py` (D7);
  - `services/monthly_report.py` (`comparison`, and removal of `_median` and of the same-day call);
  - `routers/reports.py` (the comparison route).
- **Frontend:**
  - `app/laporan/page.tsx` and `components/report/*`;
  - `components/analysis/RingkasanTab.tsx` and its cards;
  - `components/insights/ObservedInsights.tsx`;
  - `lib/queryKeys.ts`.
- **Data:** no migration and no stored data change.
- **Docs:** `docs/NEW_USER_GUIDE.md` (Laporan and Analisis) and AGENTS.md (the comparison rule).
- **Rollback:** revert the merge. Same-day logic returns with it, and no data needs restoring.
