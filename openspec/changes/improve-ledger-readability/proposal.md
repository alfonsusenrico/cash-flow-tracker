## Approval

Pending. Requested by the owner on 2026-11-16 from their review of Transaksi in production. Decisions made the same day: two-line rows, a dot on every row, and the running cycle's end estimated from the payday, never before today.

## Why

On Transaksi the transaction name is what tells the owner what a row is, but on a desktop it is cut to a few words: the account, the note icon and the tags all share its line, and each row's extras start at a different place. The pillar column marks Kakeibo pillars with a dot but income and moves without one. The "Investasi" tab repeats most of "Tabungan & investasi", and its hero, like the hero of "Masuk" and "Pindah saldo", shows figures that are always Rp 0. The running cycle reads "Siklus sejak 30 Okt · berlanjut" instead of a plain date range.

## Expected Outcome

- On a desktop, each row's name has its whole first line; the account, the note and the tags sit on a muted second line, left-aligned under the name.
- Every row's pillar column has a dot and a word: the pillar, "Uang masuk", the saving direction, or "Pindah saldo".
- The types are Semua, Pengeluaran, Masuk, Pindah saldo, Tabungan & investasi and Kredit. Investment top-ups are listed under "Tabungan & investasi".
- Each type's hero shows a figure that means something for it: Masuk shows the money in, Pindah saldo the amount moved.
- Every salary-cycle label is a date range, such as "30 Okt – 28 Nov" for the running cycle.

## What Changes

- **Rows:** desktop transaction rows become two lines when they have an account, note or tags; the phone layout is unchanged.
- **Pillar column:** a dot on every row, with the existing status colours (income mint, moves sky, investment lavender, waiting amber); a move that changes no saving figure reads "Pindah saldo" instead of "—".
- **Types:** "Investasi" leaves the rail and the type control. `type=saving` also lists top-up legs and Investasi rows; "Ditabung & investasi" (`saving_net`) is unchanged. An old `?type=investment` link opens "Tabungan & investasi". The API keeps accepting `type=investment`.
- **Hero:** "Masuk" shows the money in alone; "Pindah saldo" shows the amount moved, from a new summary field `moved`.
- **Cycle labels:** "{start} – {end}" everywhere a salary cycle is named (Transaksi, Analisis, Beranda, Laporan). A finished cycle ends on its last day. The running cycle ends on the day before the expected next salary (the existing estimate from regular receipts, otherwise the payday setting), and never before today. The label never moves a report window.

## Non-goals

- The phone layout of rows, the companion, and lists other than transaction rows (Pengaturan, Tinjau, Rekening dialogs).
- Removing `type=investment` from the API.
- Changing how a cycle is bounded: cycles still open and close only on confirmed salary receipts.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `transaction-ledger-management`: row layout, pillar column, type list, `type=saving`, hero per type, summary `moved`.
- `report-periods-and-drilldown`: salary-cycle labels as a date range with an estimated end for the running cycle.

No project capability is added. `api-consumers` applies: `type=saving` lists more rows and the summary gains `moved`; existing fields keep their meaning.

## Impact

- **Backend:** `routers/transactions.py` (`type=saving` condition and `type_counts`, summary `moved`); `services/observed_periods.py`, `report_windows.py`, `cycle_summary.py`, `monthly_report.py` (cycle labels); the period responses gain the running cycle's estimated last day; both message catalogues.
- **Frontend:** `components/workspace/lists.tsx` and `globals.css` (two-line transaction rows), `components/transactions/rows.tsx` (dots), `LedgerList.tsx`, `app/ledger/LedgerRail.tsx`, `LedgerHero.tsx`, `lib/ledgerFilters.ts` (type list and the old link), `components/ui/PeriodStepper.tsx`, `components/analysis/period.ts`, both message catalogues.
- **Data:** no migration.
- Local verification only until the owner accepts; production changes only through the merge and the deploy workflow.
