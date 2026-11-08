## MODIFIED Requirements

### Requirement: Calendar Heat Levels
The server SHALL assign each day a heat level from 0 to 5: 0 for no spending, and 1 to 5 from thresholds at the 20th, 40th, 60th and 80th percentiles of the window's days with spending, rounded to Rp 1.000. The response SHALL include the thresholds. The interface SHALL write each day's amount and SHALL show its level as a 4 px strip whose strength follows the level, with a legend naming the lowest and the highest threshold.

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

## ADDED Requirements

### Requirement: Kalender on the Phone
Below 768 CSS pixels Kalender SHALL show the whole grid of the period with cells at least 56 px high and the day panel after it, with no horizontal overflow at 390 or 320 CSS pixels.

#### Scenario: Phone grid
- **WHEN** Kalender is shown at 390 CSS pixels
- **THEN** every week of the period SHALL be visible as a grid row with compact amounts, and selecting a day SHALL update the panel below

#### Scenario: Narrow phone
- **WHEN** Kalender is shown at 320 CSS pixels
- **THEN** every cell's amount SHALL fit inside its cell and no content SHALL overflow horizontally

## REMOVED Requirements

### Requirement: Kalender Phone Week Strip
**Reason**: The prototype shows the whole month on the phone; a week at a time hides the period's shape.
**Migration**: "Kalender on the Phone" replaces it. Selecting a day and the keyboard walk work the same on the phone grid.
