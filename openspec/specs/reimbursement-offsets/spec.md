# reimbursement-offsets Specification

## Purpose
Treat money a friend pays back for a shared expense as a reduction of that spending instead of
income, so income, category spending, Kakeibo and safe-to-spend stay truthful.

## Requirements

### Requirement: Reimbursement Category
Income categories SHALL carry an `offsets_spending` flag, false by default and never true on an expense category. Every user SHALL have the income category "Patungan & Ganti Uang" with the flag set. A transaction is a reimbursement when it is income in a category with the flag set. The category API SHALL return the flag.

#### Scenario: Existing and new users get the category
- **WHEN** the migration runs, or a new user registers
- **THEN** the user SHALL have the income category "Patungan & Ganti Uang" with `offsets_spending = true`.

#### Scenario: Flag on an expense category is rejected
- **WHEN** any writer stores an expense category with `offsets_spending = true`
- **THEN** the database SHALL reject the write.

#### Scenario: Reimbursement category cannot become an expense category
- **WHEN** the owner changes the kind of "Patungan & Ganti Uang" to expense
- **THEN** the API SHALL respond with 409 and the category SHALL be unchanged.

### Requirement: Offset Category Link
A reimbursement MAY name the expense category it repays in `offset_category_id`. The API SHALL accept it on create and update only for income in an offsetting category, and only for an active, budget-reportable expense category of the same owner; otherwise it SHALL respond with 422. The database SHALL reject an offset on an expense row.

#### Scenario: Owner links a repayment to dinner spending
- **WHEN** the owner records Rp 200.000 income in "Patungan & Ganti Uang" with offset category "Makan Bareng"
- **THEN** the row SHALL be stored with that offset category.

#### Scenario: Offset on an expense is rejected
- **WHEN** a client creates or updates an expense with an offset category
- **THEN** the API SHALL respond with 422 code `offset_not_allowed` and no row SHALL change.

#### Scenario: Offset on ordinary income is rejected
- **WHEN** a client records income in "Gaji" with an offset category
- **THEN** the API SHALL respond with 422 code `offset_not_allowed`.

#### Scenario: Offset to another owner's category is rejected
- **WHEN** a client sends an offset category that belongs to a different user
- **THEN** the API SHALL respond with 422 code `invalid_offset_category`.

#### Scenario: Offset to an income or non-reportable category is rejected
- **WHEN** a client sends an income category, an excluded category, or a category that reports leave out by name ("Internal Movement", "Investasi") as the offset
- **THEN** the API SHALL respond with 422 code `invalid_offset_category`.

#### Scenario: Recategorising away clears the offset
- **GIVEN** a reimbursement with offset category "Makan Bareng"
- **WHEN** the owner changes its category to "Gaji" without sending an offset
- **THEN** the offset category SHALL be cleared.

#### Scenario: Offset category in use is archived, not deleted
- **WHEN** the owner deletes an expense category that a reimbursement names as its offset
- **THEN** the category SHALL be archived and the link SHALL remain.

### Requirement: Reimbursements Are Not Income
Every income total SHALL exclude reimbursements: dashboard overview and analytics totals and comparisons, daily series, pulse cycle income, the insights totals and the ledger summary inflow. Reports SHALL return the sum of reimbursements without an offset category as `reimbursement_unallocated`, and these SHALL change no spending figure.

#### Scenario: Unallocated reimbursement
- **GIVEN** a cycle with salary Rp 5.000.000 and a Rp 80.000 reimbursement without an offset category
- **WHEN** the dashboard overview is requested
- **THEN** `total_inflow` SHALL be Rp 5.000.000
- **AND** `reimbursement_unallocated` SHALL be Rp 80.000
- **AND** `total_outflow` SHALL be unchanged by it.

### Requirement: Reimbursements Reduce Spending of Their Offset Category
For a report window, the applied offset of category X SHALL be the smaller of X's reportable expenses and reimbursements naming X in the window. Total spending and X's category spending SHALL be reduced by that amount. The applied amount SHALL be allocated proportionally among the effective pillars of X's expenses with exact integer totals and deterministic rounding.

#### Scenario: Shared dinner partly repaid
- **GIVEN** salary Rp 5.000.000, Rp 300.000 spent in a want category, Rp 200.000 in a need category, a Rp 200.000 reimbursement offsetting the want category and Rp 80.000 unallocated reimbursement in the same window
- **WHEN** dashboard overview is requested
- **THEN** inflow SHALL be Rp 5.000.000, gross outflow Rp 500.000, applied offset Rp 200.000, net outflow Rp 300.000, want spending Rp 100.000 and need spending Rp 200.000
- **AND** the unallocated reimbursement SHALL not reduce spending

#### Scenario: Analytics follow the same figures
- **WHEN** the same shared-dinner window is requested in analytics
- **THEN** net outflow and category spending SHALL agree with overview

#### Scenario: Mixed transaction pillars
- **GIVEN** a category has Rp 100.000 of need and Rp 300.000 of want expenses, and a category-linked reimbursement of Rp 200.000
- **WHEN** Kakeibo amounts are calculated for that window
- **THEN** the offset SHALL reduce need by Rp 50.000 and want by Rp 150.000
- **AND** total net pillar spending SHALL equal Rp 200.000

#### Scenario: Category without its own pillar
- **WHEN** an expense has no transaction or category pillar
- **THEN** its effective pillar SHALL remain `need`

### Requirement: Excess Reimbursement
When reimbursements naming X exceed X's reportable expenses in the window, X's spending and the reduction of its pillar SHALL floor at zero. The remainder SHALL be returned as `reimbursement_excess`; it SHALL count neither as income nor as a reduction of any other category, and SHALL NOT carry into another window.

#### Scenario: Repayment larger than the spending in the window
- **GIVEN** a cycle with salary Rp 5.000.000, Rp 100.000 spent in "Makan Bareng" and a Rp 150.000 reimbursement offsetting it
- **WHEN** the dashboard overview is requested
- **THEN** "Makan Bareng" SHALL show `spent` 0 and `want_spent` SHALL be 0
- **AND** `reimbursement_excess` SHALL be Rp 50.000
- **AND** `total_inflow` SHALL be Rp 5.000.000.

### Requirement: Reimbursements in Cycle Spending and Safe-to-Spend
The system SHALL apply eligible reimbursements to observed cycle spending according to its reporting scope, never deriving a daily allowance from an assumed cycle end; the one daily allowance, Beranda "Aman dipakai hari ini", uses net spending and the expected salary date. The essential monthly cost SHALL use each period's essential spending net of the `need` share of applied reimbursements, as defined by `emergency-cost-estimation`.

#### Scenario: Repayment restores safe-to-spend
- **WHEN** a cycle contains reportable spending of Rp 500.000 and an applicable Rp 200.000 offset
- **THEN** cycle spending SHALL report Rp 300.000 net and disclose the Rp 200.000 offset
- **AND** it SHALL NOT infer a daily allowance from a guessed payday
- **AND** when an expected salary date exists, Beranda's remaining amount SHALL be Rp 200.000 higher than without the offset

#### Scenario: Essential cost is net of the need share
- **WHEN** a completed period has Rp 100.000 of `need` and Rp 300.000 of `want` spending in one category and a Rp 200.000 reimbursement offsetting it
- **THEN** that period's essential spending SHALL be reduced by Rp 50.000

### Requirement: Choosing the Offset Category in Forms
Quick capture and the ledger edit dialog SHALL show an expense-category select labelled "Mengurangi pengeluaran" when the chosen income category offsets spending, SHALL list only active budget-reportable expense categories, SHALL allow leaving it empty, and SHALL send the choice as `offset_category_id`.

#### Scenario: Quick capture of a repayment
- **WHEN** the owner chooses income and the category "Patungan & Ganti Uang"
- **THEN** the "Mengurangi pengeluaran" select SHALL appear and be reachable by keyboard
- **AND** submitting SHALL send the chosen expense category as `offset_category_id`.

#### Scenario: Ordinary income hides the select
- **WHEN** the owner chooses the income category "Gaji"
- **THEN** no offset select SHALL be shown and no offset SHALL be sent.

### Requirement: Reimbursement-Net Series and Ledger Summary
Dashboard daily series (analytics `daily_cadence`, overview `cumulative_trendline` and `burn_cadence`) and the analytics day-of-week heatmap SHALL subtract applied reimbursement offsets.

#### Scenario: Series add up to the net total
- **GIVEN** the "Shared dinner partly repaid" cycle
- **WHEN** dashboard analytics and overview are requested
- **THEN** the sum of `daily_cadence[].expense` and of the heatmap `total_spent` SHALL equal `total_outflow` Rp 300.000
- **AND** the last `cumulative_outflow` of the overview trendline SHALL be Rp 300.000
- **AND** `projected_30d_outflow` SHALL be Rp 300.000.

#### Scenario: Offset spread over spending days
- **GIVEN** Rp 60.000 and Rp 240.000 spent in "Makan Bareng" on two days and a Rp 150.000 reimbursement offsetting it
- **WHEN** 30-day analytics are requested
- **THEN** the two days SHALL show Rp 30.000 and Rp 120.000.

#### Scenario: Ledger summary follows the dashboard
- **GIVEN** the "Shared dinner partly repaid" cycle
- **WHEN** the unfiltered ledger is listed
- **THEN** the cumulative `outflow` SHALL be Rp 300.000 with `gross_outflow` Rp 500.000 and `reimbursement_offset` Rp 200.000
- **AND** filtered to "Makan Bareng" the `outflow` SHALL be Rp 100.000.

#### Scenario: Filters that no report figure covers stay gross
- **WHEN** the same ledger is filtered by account
- **THEN** the `outflow` SHALL be Rp 500.000 and `reimbursement_offset` 0.

### Requirement: Offset Attribution to Spending Days
Each category's applied offset SHALL be attributed to the days that category was spent on in the window, in proportion to that day's spending, so that no day is negative and the days sum to the window's applied offset.

#### Scenario: Single spending day
- **GIVEN** Rp 90.000 spent in "Makan Bareng" on one day of the window and an applied offset of Rp 30.000 for that category
- **WHEN** 30-day analytics are requested
- **THEN** that day SHALL show Rp 60.000 and no day SHALL be negative.

### Requirement: Reimbursement-Net Burn Rates
The analytics burn rates SHALL subtract the applied offsets of their own rolling window.

#### Scenario: Offset outside the window
- **GIVEN** one reimbursement offset applied inside a burn rate's rolling window and another applied only before that window
- **WHEN** the burn rate is computed
- **THEN** it SHALL subtract the first offset and not the second.

### Requirement: Reimbursement-Net Ledger Outflow
The ledger summary outflow SHALL subtract the window's applied offsets (all, the filtered category's, or the filtered pillar's, floored at zero) when no account, goal, debt, source, review-status, search or uncategorised filter is set, the type filter is empty or expense, and not both a category and a pillar are filtered; otherwise it SHALL stay gross.

#### Scenario: Category and pillar together stay gross
- **GIVEN** the "Shared dinner partly repaid" cycle
- **WHEN** the ledger is filtered to "Makan Bareng" and pillar `need` at the same time
- **THEN** the `outflow` SHALL be the gross spending of the matching rows.

### Requirement: Ledger Summary Gross and Offset Fields
Ledger summaries SHALL return `gross_outflow` and `reimbursement_offset`.

#### Scenario: Fields present on a gross summary
- **WHEN** the ledger is filtered by search text
- **THEN** the summary SHALL contain `gross_outflow` equal to `outflow` and `reimbursement_offset` 0.

### Requirement: Reimbursements Linked to Expenses
A reimbursement SHALL be able to link to 1 to 20 of the owner's expenses. Its amount SHALL be allocated to them by expense date then id, each taking at most what it cost less what earlier reimbursements already hold on it. A linked amount SHALL lower its expense's category and effective pillar in the period of the expense's date, whatever the reimbursement's date, and a reimbursement with links SHALL ignore its category link.

#### Scenario: Repaid in the next cycle
- **GIVEN** three CG Fun expenses of Rp 60.600, Rp 193.555 and Rp 138.225 on 28–29 October in Makanan & Minuman, Budaya, and a cycle that starts on 30 October
- **WHEN** four Rp 75.000 reimbursements of 31 October link to all three
- **THEN** the previous cycle's Pengeluaran, Makanan & Minuman and Budaya SHALL fall by Rp 300.000, allocated 60.600, 193.555 and 45.845 by date, and the current cycle's spending SHALL not change

#### Scenario: Only the expense it repaid
- **GIVEN** a Rp 25.500 Belanja expense with the Tak Terduga pillar and other Belanja expenses in the same cycle
- **WHEN** a reimbursement links to that expense alone
- **THEN** Tak Terduga SHALL fall by Rp 25.500 and no other Belanja expense or pillar SHALL change

#### Scenario: Two reimbursements on one expense
- **GIVEN** a Rp 205.350 expense and two Rp 68.450 reimbursements dated 21.38 and 21.40
- **WHEN** both link to it
- **THEN** each SHALL hold Rp 68.450 and the expense SHALL count Rp 68.450 in Pengeluaran

#### Scenario: Never below zero
- **WHEN** reimbursements linked to an Rp 80.000 expense add up to Rp 120.000
- **THEN** the expense SHALL count Rp 0, the later reimbursement SHALL hold only what was left, and its rest SHALL be unallocated

#### Scenario: Not the owner's or not an expense
- **WHEN** a link names another owner's transaction, an income, a movement leg, or more than 20 expenses, or the reimbursement's category does not offset spending
- **THEN** the API SHALL answer 422 with a code in both catalogues and change nothing

### Requirement: The Rest of a Linked Reimbursement
When linking, the owner SHALL choose what happens to the amount beyond what the links hold: not counted, which is the default and reports it as unallocated, or recorded as income in an income category the owner picks. Recording it SHALL lower the reimbursement to what its links hold and write a separate income row with the rest on the same account, date and notes, so no balance changes.

#### Scenario: A gift on top of a repayment
- **GIVEN** a Rp 100.000 reimbursement and a Rp 25.500 expense
- **WHEN** the owner links them and records the rest as income in "Pendapatan Lain"
- **THEN** the reimbursement SHALL be Rp 25.500, a Rp 74.500 income in "Pendapatan Lain" SHALL exist on the same account and date, that period's Masuk SHALL rise by Rp 74.500, and the account balance SHALL be unchanged

#### Scenario: Rest not counted
- **WHEN** the owner keeps the default
- **THEN** the reimbursement SHALL keep its amount and the rest SHALL be reported as `reimbursement_unallocated`

### Requirement: Links Follow Changes
Editing the amount, date, type or category of a linked expense or reimbursement, or deleting either, SHALL recompute the allocations of every reimbursement sharing the expenses involved. An expense that is no longer an expense, or a reimbursement whose category no longer offsets spending, SHALL lose its links.

#### Scenario: Expense lowered
- **GIVEN** a Rp 80.000 expense with a Rp 40.000 reimbursement linked
- **WHEN** the expense is edited to Rp 30.000
- **THEN** the link SHALL hold Rp 30.000 and Rp 10.000 SHALL be unallocated

#### Scenario: Reimbursement deleted
- **GIVEN** two reimbursements linked to one expense, the second holding less than its amount
- **WHEN** the first is deleted
- **THEN** the second SHALL be recomputed and hold up to its full amount

### Requirement: Linking in the Web App
The reimbursement's edit dialog and Catat SHALL offer "Mengganti transaksi" for an income in an offsetting category: a search over the owner's expenses of the last 90 days showing title, date and amount, choosing several. After saving, the dialog SHALL show each link's allocated amount and the rest with its choice. An expense's transaction dialog SHALL show "Diganti Rp X" with each reimbursement.

#### Scenario: Linking from the edit dialog
- **WHEN** the owner opens a Rp 100.000 reimbursement, picks "beli kopi titipan mama" Rp 25.500, chooses "Dicatat sebagai uang masuk" with "Pendapatan Lain" and saves
- **THEN** `PUT /api/transactions/{id}/reimburses` SHALL be sent with the expense and the rest's category, and the dialog SHALL show Rp 25.500 linked and Rp 74.500 recorded as income

#### Scenario: Seeing it on the expense
- **WHEN** the owner opens "beli kopi titipan mama"
- **THEN** its dialog SHALL read "Diganti Rp 25.500" with the reimbursement's date and note

#### Scenario: Keyboard and screen reader
- **WHEN** the picker is used by keyboard
- **THEN** the search, each result and the chosen list SHALL be reachable and named, and the choice SHALL be announced
