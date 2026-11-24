# Design: improve-daily-spending-detail

## Context

`daily_cadence` (`routers/dashboard.py`) gives each local day of the period `spending` from `spending_basis(...)`, the same Pengeluaran Analisis shows everywhere (reportable expenses, net of applied reimbursements, without saving-pillar rows). `DailySpendingChart` draws one bar per day with a hover/tap tooltip and a "Lihat sebagai tabel" date + amount table. Kalender already opens a day from the URL (`/insights?tab=kalender&day=YYYY-MM-DD`) and shows that day's transactions.

The comparison row grid is `minmax(0,1fr) 120px 72px 120px`; "seluruh periode" at 12 px is about 95 px wide, so it overflows into the bar column.

## Decisions

### 1. A bar opens Kalender

Each bar becomes a `<button>` named "<date>, <amount>, buka di Kalender". Click, Enter or Space navigates to Kalender at that day with the current basis and offset (the page's existing `go("kalender", {day})`). Hover keeps the tooltip. On a touch screen the first tap shows the tooltip with a "Lihat di Kalender" link; tapping the link opens the day, and tapping the bar again hides the tooltip, as today. A day with Rp 0 is still a button, so an empty day can be checked.

Rejected: opening Transaksi for the day (owner chose Kalender, which already lists the day with its totals).

### 2. Breakdown from the same basis

`spending_basis` is extended, not duplicated: its per-day rows keep the category (and therefore the effective pillar, as Kakeibo uses), and the reimbursement offset of a day is applied to the categories it was applied to. Each day gains:
- `by_pillar: {need, want, culture, unexpected, unclassified}`;
- `by_category: [{category_id, amount}]` (amount > 0 only).

Both sum exactly to the day's `spending`; a test checks every day of a seeded period, including a day with a reimbursement.

Rejected: computing the split in the browser from ledger rows (it would not net reimbursements the way the figure does).

### 3. The table and its switch

Above the table, a two-option switch "Per pilar | Per kategori" (a radio group, default Per pilar, remembered per browser).
- Per pilar: Tanggal, Kebutuhan, Keinginan, Budaya, Tak Terduga, Belum diklasifikasi, Total. "Belum diklasifikasi" appears only when the period has some.
- Per kategori: Tanggal, the period's five largest categories by Pengeluaran (names as the owner wrote them), "Lainnya" for the rest, Total. Fewer than six categories show all of them and no "Lainnya".
- A zero cell reads "–" in muted text; Total stays bold; the highest day keeps "· tertinggi".
- The table scrolls horizontally inside its box below the needed width, with the date column fixed; the page never scrolls sideways.

### 4. Comparison row

The third column becomes `max-content` (minimum 72 px), so "seluruh periode" and "whole period" fit on one line before the bar.

## Risks

- Category names can be long; header cells wrap to two lines at most and each cell carries the full name as its accessible header.
- The breakdown makes `daily_cadence` larger (at most about 40 days × a few categories); acceptable.

## Verification

- Backend: `by_pillar` and `by_category` sum to `spending` on every day of a seeded cycle, including a reimbursed day; existing analytics tests unchanged.
- Web: bar buttons (click, keyboard, touch link) navigate to Kalender at the day; the switch and both column sets; zero cells; the comparison row label.
- Browser: Ringkasan at 1440, 1024 and 390 px, light and dark; axe; no page-level horizontal scroll.
