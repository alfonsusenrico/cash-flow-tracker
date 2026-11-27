# cycle-calendar Specification

## Purpose
Show each day of a period with its spending, waiting items and due bills, so the days add up exactly to the period's Belanja.

## Requirements

### Requirement: Calendar Endpoint
`GET /api/dashboard/calendar` SHALL take `timeframe` (`cycle` or `month`) and `cycle_offset` (−24 to 12). It SHALL return the window, the expected salary date or the reason it is unavailable, and one entry per local day of the window. Each entry SHALL hold the day's spending, heat level, income, movement, top-up and waiting counts and amounts, and its plan occurrences. The response SHALL be owner-scoped, and an invalid parameter SHALL answer 422.

#### Scenario: Whole window
- **WHEN** the calendar of a 31-day cycle is requested on its ninth day
- **THEN** `days` SHALL hold 31 entries, and the days after today SHALL have zero spending and only their plans

#### Scenario: Invalid timeframe
- **WHEN** the request has `timeframe=30d`
- **THEN** the response SHALL be 422

#### Scenario: Another owner
- **GIVEN** owner B has transactions in the same window
- **WHEN** owner A requests the calendar
- **THEN** no figure, row or plan of owner B SHALL be included

### Requirement: Calendar Upcoming Bills
For the current period, the calendar SHALL return the unmet plan occurrences from today to the expected salary date, or to the window end when that date is unavailable, ordered by date and with their total computed by the server. For other periods the list SHALL be empty.

#### Scenario: Bills before the next salary
- **GIVEN** an unpaid "Internet Rumah" plan of Rp 350.000 due in three days and the next salary expected in sixteen days
- **WHEN** the current period's calendar is requested
- **THEN** `upcoming` SHALL list it with status `due`, and `upcoming_total` SHALL include Rp 350.000

#### Scenario: Past period
- **WHEN** the previous cycle's calendar is requested
- **THEN** `upcoming` SHALL be empty

### Requirement: Calendar Heat Levels
The server SHALL assign each day a heat level from 0 to 5: 0 without spending, else 1 to 5 from the 20th, 40th, 60th and 80th percentiles of the window's days with spending, rounded to Rp 1.000, and SHALL return the thresholds, on the Belanja and the Pengeluaran basis (`spending_heat`, `spending_heat_thresholds`). Kalender SHALL write each day's Pengeluaran and show its level as a 4 px strip, with a legend naming the lowest and the highest threshold.

#### Scenario: Thresholds in the legend
- **WHEN** the Kalender tab is shown
- **THEN** its legend SHALL show five strip steps from "sampai {first threshold}" to "di atas {last threshold}", and every day with spending SHALL show its amount in text

#### Scenario: Strip follows the server level
- **WHEN** a day has heat level 2
- **THEN** its strip SHALL be drawn at the second strength, and a day at level 0 SHALL have no strip

### Requirement: Kalender Grid
Kalender SHALL show "Pengeluaran periode ini" as its hero with the day of the period and the days left until the expected salary, or the reason it is unavailable, and a grid of weeks starting on Monday. Each cell SHALL show the date, the compact amount and square markers for income, transfers, top-ups, bills and waiting rows. The arrow keys, Home, End, PageUp and PageDown SHALL move the day, and each cell SHALL be named with its date, spending, waiting count and plans.

#### Scenario: Keyboard walk
- **WHEN** the owner focuses the grid and presses the Right arrow then Enter
- **THEN** focus SHALL move to the next day and the day panel SHALL show its transactions

#### Scenario: Keys without single-key shortcuts
- **GIVEN** the owner turned single-key shortcuts off
- **WHEN** focus is in the grid and the owner presses the Down arrow
- **THEN** the day seven days later SHALL be selected

#### Scenario: Expected salary unavailable
- **GIVEN** only two salary receipts are recorded
- **WHEN** Kalender is shown
- **THEN** the hero line SHALL say the next salary date cannot be estimated yet and why, and the bills list SHALL run to the end of the period under "Sampai akhir periode"

#### Scenario: Marker contrast
- **WHEN** Kalender is shown in either theme at 1440, 1024, 768, 390 and 320 px, with a day selected and a day hovered
- **THEN** every marker SHALL measure at least 3:1 against its cell, the waiting marker SHALL be outlined and the bill marker filled, and axe SHALL report no colour-contrast finding

### Requirement: Kalender Day Sheet
Selecting a day SHALL show in the panel that day's Pengeluaran, its transactions, its waiting rows and its plans. Each transaction SHALL open the shared transaction dialog, each waiting row SHALL link to its Tinjau item, and the panel SHALL link to Transaksi filtered to that day. For the current period Kalender SHALL also list the bills due before the next salary with the server's total.

#### Scenario: Waiting row link
- **WHEN** the selected day has a waiting row
- **THEN** the panel SHALL list it under "Menunggu jawaban" with a link to that item in Tinjau

#### Scenario: Open a transaction
- **WHEN** the owner selects a transaction in the day panel
- **THEN** the shared transaction dialog SHALL open for that row, an expense SHALL read "−Rp" in the expense text colour, and closing it SHALL return focus to the row

#### Scenario: Transaksi for the day
- **WHEN** the owner selects "Lihat di Transaksi" with 2 November selected
- **THEN** the ledger SHALL open filtered to 2 November only

### Requirement: Kalender on the Phone
Below 768 CSS pixels Kalender SHALL show the whole grid of the period with cells at least 56 px high and the day panel after it, with no horizontal overflow at 390 or 320 CSS pixels.

#### Scenario: Phone grid
- **WHEN** Kalender is shown at 390 CSS pixels
- **THEN** every week of the period SHALL be visible as a grid row with compact amounts, and selecting a day SHALL update the panel below

#### Scenario: Narrow phone
- **WHEN** Kalender is shown at 320 CSS pixels
- **THEN** every cell's amount SHALL fit inside its cell and no content SHALL overflow horizontally

### Requirement: Calendar Days at Full Price
A day's `spend` SHALL be that local day's reportable spending and its `spending` that day's Pengeluaran rows, both at full price, with no reimbursement subtracted. The days' `spend` SHALL add up to `totals.gross_spend`, and their `spending` to `totals.spending_gross`.

#### Scenario: Reimbursement on a later day
- **GIVEN** Rp 200.000 spent on 3 November in "Makan di Luar" and a Rp 50.000 reimbursement on 6 November for that category
- **WHEN** the calendar is requested
- **THEN** 3 November SHALL show Rp 200.000, 6 November no spending for it, `spending_gross` SHALL include Rp 200.000, `spending_reimbursed` SHALL be Rp 50.000, and `spending` SHALL be the period's Pengeluaran

#### Scenario: A day matches its rows
- **GIVEN** 21 November holds a Rp 190.000 meal and Rp 10.000 of parking, and Transportasi has unlinked reimbursements in the period
- **WHEN** the calendar is requested
- **THEN** 21 November's `spending` SHALL be Rp 200.000

#### Scenario: Waiting rows stay counted
- **GIVEN** a Rp 45.000 expense waiting for a purpose answer on 2 November
- **WHEN** the calendar is requested
- **THEN** 2 November's spending SHALL include Rp 45.000 and its waiting count SHALL be 1

#### Scenario: Movement counted once
- **WHEN** a linked Pindah saldo of Rp 500.000 happened on 2 November
- **THEN** 2 November SHALL report one movement of Rp 500.000, not two legs, and no spending for it

### Requirement: Calendar Reimbursed Totals
The calendar `totals` SHALL return `spending` (the period's Pengeluaran, net of reimbursements), `spending_gross` and `spending_reimbursed`, with `spending_gross − spending_reimbursed = spending`, and SHALL keep `spend`, `gross_spend` and `offset_applied` for Belanja. A response whose figures break these sums SHALL fail rather than be shown.

#### Scenario: Totals reconcile
- **GIVEN** Rp 200.000 spent on 3 November in "Makan di Luar" and a Rp 50.000 reimbursement for that category
- **WHEN** the calendar is requested
- **THEN** `spending_gross` SHALL be Rp 200.000, `spending_reimbursed` Rp 50.000 and `spending` Rp 150.000

### Requirement: Kalender Names the Reimbursed Amount
When `spending_reimbursed` is above zero, Kalender's hero SHALL show under the period's Pengeluaran the line "{spending_gross} dibelanjakan · −{spending_reimbursed} diganti" ("spent", "reimbursed" in English), masked when balances are hidden. With nothing reimbursed the line SHALL not appear.

#### Scenario: Line with reimbursements
- **GIVEN** a period with Rp 1.250.000 of full-price days and Rp 200.000 reimbursed
- **WHEN** Kalender is shown
- **THEN** the hero SHALL read Rp 1.050.000 with the line "Rp 1.250.000 dibelanjakan · −Rp 200.000 diganti"

#### Scenario: No reimbursement
- **WHEN** nothing was reimbursed in the period
- **THEN** the hero SHALL show no such line
