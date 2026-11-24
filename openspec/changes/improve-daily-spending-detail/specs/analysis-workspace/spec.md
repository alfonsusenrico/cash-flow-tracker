## MODIFIED Requirements

### Requirement: Ringkasan Daily Spending
Ringkasan SHALL show "Pengeluaran harian" as one bar per local day of the period up to today, with gridlines, the highest day in the expense mark colour and today hatched, a legend naming both with date and amount, and the same values as a table. Pointing at a bar, or tapping it on a touch screen, SHALL show that day's date and amount above the bar until the pointer leaves the chart, the bar is tapped again or Esc is pressed. Each bar SHALL be a button that opens Kalender at its day.

#### Scenario: Daily bars
- **WHEN** the current cycle is shown
- **THEN** the highest day SHALL be drawn in the expense mark colour and named in the legend with its date and amount, today SHALL be hatched and named as still running, later days SHALL be absent, and "Lihat sebagai tabel" SHALL show the same values as a table

#### Scenario: Hovering a bar
- **GIVEN** 1 November with Rp 40.000 of spending
- **WHEN** the owner points at its bar
- **THEN** "1 Nov" and "Rp 40.000" SHALL appear above the bar, inside the chart, and SHALL disappear when the pointer leaves the chart or Esc is pressed

#### Scenario: Tapping a bar
- **WHEN** the owner taps today's bar on a phone
- **THEN** its date with "hari ini", its amount and "Lihat di Kalender" SHALL appear, and a second tap SHALL hide them

#### Scenario: Labels at every width
- **WHEN** the chart is shown at 1440, 1024, 390 and 320 px
- **THEN** every axis and date label SHALL render at 12 px, no two labels SHALL overlap, and the chart SHALL not overflow its column

#### Scenario: A bar opens its day
- **WHEN** the owner clicks the bar of 4 November, or focuses it and presses Enter
- **THEN** Analisis SHALL switch to Kalender with 4 November selected in the same period, showing that day's transactions

## ADDED Requirements

### Requirement: Daily Spending by Pillar and Category
Each day of `daily_cadence` SHALL carry `by_pillar` (need, want, culture, unexpected, unclassified) and `by_category` (category id, name and amount), computed from the same basis as its `spending`, each adding up exactly to it. The table SHALL offer "Per pilar" (the pillars and Total) and "Per kategori" (the period's five largest categories, "Lainnya" and Total), and SHALL scroll inside its box with the date fixed when narrow.

#### Scenario: Pillars add up
- **GIVEN** a day with Rp 412.000 of needs, Rp 677.649 of wants and a reimbursement of Rp 50.000 applied to a want
- **WHEN** the period's analytics are requested
- **THEN** that day's `by_pillar` SHALL be need 412.000 and want 627.649, and their sum SHALL equal the day's `spending`

#### Scenario: Per category columns
- **GIVEN** a period with eight spending categories
- **WHEN** the owner switches the table to "Per kategori"
- **THEN** the columns SHALL be the five largest categories of the period, "Lainnya" holding the other three, and Total, and each row SHALL add up to its Total

#### Scenario: Narrow screen
- **WHEN** the table is shown at 390 px in "Per pilar"
- **THEN** the table SHALL scroll sideways inside its box with the date column fixed, and the page SHALL not scroll sideways

### Requirement: Comparison Label Fits
The comparison row's "seluruh periode" (or "whole period") label SHALL fit on one line beside its bar without touching it at every desktop width.

#### Scenario: Label and bar apart
- **WHEN** Ringkasan shows "135% dari total 29 Sep – 30 Okt" at 1280 px
- **THEN** "seluruh periode" SHALL end before its bar starts, with at least 8 px between them
