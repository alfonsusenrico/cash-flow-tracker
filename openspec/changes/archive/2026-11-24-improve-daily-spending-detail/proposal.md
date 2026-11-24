## Approval

Approved by the owner on 2026-11-24 ("ok approved, build it"). Requested the same day from their review of Analisis → Ringkasan. Decisions made the same day: the table splits each day by Kakeibo pillar by default with a switch to the period's top categories; a bar opens Kalender at that day; saving and investing stay in the line under the hero, not in the Kakeibo rows.

## Why

On Ringkasan, "Pengeluaran harian" shows only a total per day: a tall bar or a large row says a day was expensive, not what it was spent on, and the owner has to switch to Kalender and find the day by hand. The comparison row also clips: "seluruh periode" runs into its bar because its column is 72 px and the text needs about 95 px.

## Expected Outcome

- Clicking a bar of "Pengeluaran harian" opens Kalender at that day, with its transactions.
- "Lihat sebagai tabel" shows each day split into Kebutuhan, Keinginan, Budaya, Tak Terduga and Belum diklasifikasi with the total; "Per kategori" switches the columns to the period's five largest categories and "Lainnya". Every row still adds up to the day's Pengeluaran.
- "seluruh periode" and its bar no longer touch.

## What Changes

- **Bars open Kalender:** each bar is a button; click, Enter or Space opens `/insights?tab=kalender&day=<date>` with the same period. Hover still shows the date and amount; on a touch screen the first tap shows them with "Lihat di Kalender", which opens the day.
- **Daily breakdown in the API:** each `daily_cadence` day gains `by_pillar` (need, want, culture, unexpected, unclassified) and `by_category` (category id, name and amount), both from the same spending basis as `spending`, so each adds up to it exactly.
- **Table:** a "Per pilar | Per kategori" switch above the table; per pillar shows the five pillar columns and Total; per category shows the period's top five categories by Pengeluaran, "Lainnya" for the rest, and Total. On narrow screens the table scrolls inside its box with the date column fixed.
- **Fix:** the comparison row's "seluruh periode" column fits its text.

## Non-goals

- A saving or investment column or Kakeibo row (owner decision): saving stays in the hero line and the chart stays "Di luar tabungan dan investasi".
- Clicking a table row (the bar is the entry point; the table stays a data view).
- Kalender changes.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `analysis-workspace`: "Ringkasan Daily Spending" (bars open Kalender; the table's breakdown and switch).

No project capability is added. `api-consumers` applies: `daily_cadence` gains two fields; existing fields are unchanged.

## Impact

- **Backend:** `routers/dashboard.py` (`daily_cadence`) and the spending-basis helper it uses; tests.
- **Frontend:** `components/analysis/DailySpendingChart.tsx` (buttons, table switch and columns), `RingkasanTab.tsx` (navigation), `app/globals.css` (comparison row column, table scroll), both message catalogues; tests.
- **Data:** no migration.
