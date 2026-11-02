## ADDED Requirements

### Requirement: Monthly report periods
The system SHALL offer one monthly report ("Laporan Bulanan") per period at `GET /api/reports/monthly`, identified by its basis and local `start_date`. The basis SHALL be the calendar month (`basis=month`, the default) or, for a user with confirmed salary cycles, the closed salary cycle (`basis=cycle`). Without `start_date` the report SHALL be the latest closed period, else the period in progress.

#### Scenario: Default month report
- **GIVEN** transactions recorded since 15 September 2026, viewed on 2 November 2026
- **WHEN** the report is requested without a period
- **THEN** it SHALL be "Oktober 2026" from 1 to 31 October in the application timezone

#### Scenario: Salary cycle basis
- **GIVEN** confirmed salary receipts on 25 August, 25 September and 24 October
- **WHEN** the report is requested with `basis=cycle`
- **THEN** it SHALL cover the closed cycle from the 25 September receipt to the instant before the 24 October receipt, with local dates 25 September to 24 October, and the list SHALL contain only salary cycles

#### Scenario: No salary cycle
- **WHEN** a user without confirmed salary receipts requests `basis=cycle`
- **THEN** the response SHALL be 409 `cycle_unavailable` and the periods list SHALL say `cycle_available: false`

#### Scenario: Unknown period
- **WHEN** the report is requested with a `start_date` that is not a listed period start
- **THEN** the response SHALL be 404 `period_not_found`

### Requirement: Past report list
`GET /api/reports/monthly/periods` SHALL list, newest first, the period in progress marked `complete: false` and up to 24 closed periods of the basis, back to the period in which the first transaction was recorded. That earliest period SHALL carry `tracking_started_inside: true` and the first recorded date when tracking began after its first day.

#### Scenario: List with partial first month
- **GIVEN** transactions recorded since 15 September 2026, viewed on 2 November 2026
- **WHEN** the month list is requested
- **THEN** it SHALL contain November 2026 (in progress), October 2026 and September 2026 with `tracking_started_inside: true` and first recorded date 15 September

### Requirement: Four Kakeibo questions in a fixed order
The report SHALL answer, in this order, "Berapa uang masuk?", "Berapa yang ditabung?", "Berapa yang dibelanjakan?" and "Apa yang mau diperbaiki bulan depan?". The first SHALL give reportable inflow and its sources by category. The second SHALL give fresh savings with its saving-classified expenses and net savings movements, separate from the cash-flow surplus. The last SHALL hold the reflection note. Every figure SHALL come from the existing reporting services and their rules.

#### Scenario: Savings separate from surplus
- **GIVEN** a month with Rp 5.000.000 inflow, Rp 3.000.000 outflow, a Rp 500.000 movement into a goal-linked pocket and a Rp 100.000 saving-classified expense
- **WHEN** the report is requested
- **THEN** fresh savings SHALL be Rp 600.000 with both components listed, and the surplus SHALL be Rp 2.000.000, labelled as not proof that money was set aside

### Requirement: Spending answer
"Berapa yang dibelanjakan?" SHALL give net outflow, spending in the five Kakeibo pillars and "Belum diklasifikasi", the top five categories and merchants, the "Tak Terduga" items, the recurring charges detected in the period, and, briefly, the current essential monthly cost and emergency coverage labelled "saat ini".

#### Scenario: Unexpected items
- **WHEN** the month has three expenses with the effective pillar `unexpected`
- **THEN** the report SHALL list them with date, amount and description, and their total SHALL equal the `unexpected` pillar amount

#### Scenario: Recurring charges in the period
- **WHEN** a monthly charge was last seen inside the period
- **THEN** it SHALL be listed with its merchant, cadence and typical amount, and charges last seen before the period SHALL NOT be listed

### Requirement: Report equals the ledger
The report's inflow, gross outflow, reimbursement offset, outflow and net, each pillar amount, each category amount and each income source SHALL equal the summary of `GET /api/transactions` for the same `from_date`/`to_date` window with the matching filters. No report figure SHALL use a calculation rule the ledger summary does not use.

#### Scenario: Reconciliation
- **GIVEN** a month with income, expenses in several pillars and categories, an unclassified expense, an uncategorised expense, an internal movement with a fee, an investment trade and a reimbursement
- **WHEN** the report and the ledger summaries are requested for the same window
- **THEN** every listed amount SHALL be equal, the pillar amounts SHALL add up to the outflow and the category amounts SHALL add up to the outflow

### Requirement: Comparison with the previous period
The report SHALL compare the period with the previous period of the same basis using the fair period comparison of `observed-insights`, stating the compared days, and SHALL set `previous_partial: true` when tracking began inside the previous period. No percentage change SHALL be shown.

#### Scenario: Partial previous month
- **WHEN** the October report is compared with a September in which tracking began on the 15th
- **THEN** the comparison SHALL carry `previous_partial: true` and the interface SHALL say that September was recorded only from 15 September

### Requirement: Three-period median
The report SHALL compare inflow, outflow and net with the median of the three most recent full periods before it, all measured over their first `D` local days, where `D` is the shortest of those periods and the report period. With fewer than three full periods the median SHALL be unavailable and the response SHALL state how many periods exist.

#### Scenario: Median of three months
- **GIVEN** July, August and September with outflow of Rp 2.000.000, Rp 9.000.000 and Rp 2.400.000 in their first 30 days, and October with Rp 2.600.000 in its first 30 days
- **WHEN** the October report is requested
- **THEN** the median outflow SHALL be Rp 2.400.000 over 30 days and the current figure Rp 2.600.000 over the same 30 days

#### Scenario: Not enough history
- **WHEN** only one full period precedes the report period
- **THEN** the median SHALL be unavailable with `period_count: 1` and the interface SHALL say that three full periods are needed

### Requirement: Data quality box
The report SHALL show "Kualitas data" for its window with the number of rows waiting in Tinjau, the count and amount of unclassified spending, the number of possible duplicates (expense or income rows with another row of the same account, amount and type within 3 minutes, excluding movement legs and movement fees) and the days without any recorded transaction, each linking to its fix: Tinjau, the ledger filtered to the exact rows or "Lengkapi klasifikasi", and Catat.

#### Scenario: Possible duplicate
- **GIVEN** two Rp 25.000 expenses on the same account 2 minutes apart and a Rp 25.000 movement leg on that account 1 minute later
- **WHEN** the report is requested
- **THEN** possible duplicates SHALL be 2, and `GET /api/transactions?possible_duplicates=true` for the window SHALL list exactly those two rows

#### Scenario: Days without records
- **WHEN** a closed month has transactions on every day except the 3rd and the 17th
- **THEN** days without records SHALL be 2 with those dates

### Requirement: Drill-down from every report figure
Every amount in the report that is a sum of ledger rows SHALL link to the ledger filtered to its exact window (`from`, `to`) and rows: `type`, `category` (or `uncategorized`), `kakeibo` (including `unclassified`), `q` for a merchant or recurring charge, `review` or `dup=1`. A figure that is not a sum of rows in one window, the essential cost and the net savings movements, SHALL say where it is explained instead of linking to rows that do not add up to it.

#### Scenario: Pillar link
- **WHEN** the owner selects the Rp 300.000 "Belum diklasifikasi" amount of the October report
- **THEN** the ledger SHALL open with `from` and `to` of October, `type=expense` and `kakeibo=unclassified` and its summary outflow SHALL be Rp 300.000

### Requirement: Reflection note per period
The owner SHALL be able to save one reflection note of 1 to 1000 characters per basis and period ("Apa yang mau diperbaiki bulan depan?") with `PUT /api/reports/monthly/reflection` and remove it with `DELETE`. Notes SHALL be stored in `monthly_reflections` (Flyway V40), scoped to their owner, and returned only to that owner. The report SHALL return the period's note and the previous period's note, which the interface SHALL show again as last period's intention.

#### Scenario: Shown again next month
- **GIVEN** the owner saved "Kurangi jajan kopi" on the October report
- **WHEN** the owner opens the November report
- **THEN** it SHALL show "Kurangi jajan kopi" as the note from October

#### Scenario: Owner scope
- **WHEN** another owner saves, reads or deletes a note for the same basis and period
- **THEN** each owner SHALL see only their own note and a delete SHALL never remove another owner's note

#### Scenario: Invalid note
- **WHEN** the note is blank or longer than 1000 characters, or the period is not listed
- **THEN** the response SHALL be 422 for the note and 404 `period_not_found` for the period, and nothing SHALL be stored

### Requirement: Monthly readiness API
`GET /api/reports/monthly/latest`, with session or Bearer authentication, SHALL return for the owner's basis (`cycle` when available, else `month`, or the requested `basis`) the latest closed period with its label, start, end and local dates, `ready: true`, the pending Tinjau count (`pending_review`, as the Tinjau badge), the `auto` count (`unreviewed`) and the web path of that report. Without a closed period it SHALL return `ready: false` and `period: null`.

#### Scenario: Ready after the month closes
- **GIVEN** activity in October and three rows needing review
- **WHEN** the phone calls the API on 2 November with its Bearer key
- **THEN** it SHALL return `ready: true`, the "Oktober 2026" window, `pending_review: 3` and `url: "/laporan?basis=month&start=2026-10-01"`, equal to the session response

#### Scenario: First month
- **WHEN** a new user's only activity is in the current month
- **THEN** the API SHALL return `ready: false` with `period: null`

#### Scenario: Unknown key
- **WHEN** the API is called with an unknown Bearer key
- **THEN** it SHALL answer 401

### Requirement: Optional budget in the report
The report SHALL return `budget: null` and show no budget figure when the owner has no category budget and no overall monthly spending budget, and SHALL then offer "Atur anggaran", a per-category monthly budget editor that saves `monthly_budget`. With a budget it SHALL show each budgeted category's budget, net spending in the period and remaining amount, and the overall budget against the outflow.

#### Scenario: No budget
- **WHEN** no category has a budget
- **THEN** the report SHALL contain no budget figure and SHALL offer "Atur anggaran"

#### Scenario: Budget set
- **GIVEN** a Rp 500.000 budget on "Jajan" and Rp 620.000 net spent in it in October
- **WHEN** the October report is requested
- **THEN** it SHALL show budget Rp 500.000, spent Rp 620.000 and Rp 120.000 over, in text

### Requirement: Report page layout
The web page `/laporan`, reachable from the navigation as "Laporan", SHALL present the report in Indonesian without verdicts, phone first and also for the desktop, in the light and the dark theme. It SHALL show the period selector and basis switch first, then the four questions in order, with the comparison, "Kualitas data" and budget beside them on wide screens and between them on the phone. It SHALL handle loading, empty and error states truthfully.

#### Scenario: Phone width
- **WHEN** the page is shown at 390 CSS pixels
- **THEN** no content SHALL overflow horizontally and every action SHALL be keyboard operable with a visible focus

#### Scenario: New user
- **WHEN** a user without any transaction opens `/laporan`
- **THEN** the page SHALL say that the first report appears after the first recorded month, without zero figures presented as results
