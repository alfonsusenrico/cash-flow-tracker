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
For a report window, the applied offset of category X SHALL be the smaller of X's reportable expenses and the reimbursements naming X, both in the window. Total spending, X's category spending and the Kakeibo pillar of X SHALL be reduced by it. The pillar SHALL follow the effective-pillar rule for a row without its own pillar in X.

#### Scenario: Shared dinner partly repaid
- **GIVEN** a cycle with salary Rp 5.000.000, Rp 300.000 spent in "Makan Bareng" (want), Rp 200.000 in "Belanja Dapur" (need), a Rp 200.000 reimbursement offsetting "Makan Bareng" and a Rp 80.000 reimbursement without an offset
- **WHEN** the dashboard overview is requested
- **THEN** `total_inflow` SHALL be Rp 5.000.000 and `gross_outflow` Rp 500.000
- **AND** `reimbursement_offset` SHALL be Rp 200.000 and `total_outflow` Rp 300.000
- **AND** "Makan Bareng" SHALL show `spent` Rp 100.000, `gross_spent` Rp 300.000 and `reimbursed` Rp 200.000
- **AND** Kakeibo SHALL show `want_spent` Rp 100.000 and `need_spent` Rp 200.000, with `want_pct` 2.0 and `need_pct` 4.0.

#### Scenario: Analytics follow the same figures
- **GIVEN** the cycle of the previous scenario
- **WHEN** dashboard analytics are requested
- **THEN** `total_outflow` SHALL be Rp 300.000 and "Makan Bareng" in the category variance SHALL show `spent` Rp 100.000.

#### Scenario: Category without its own pillar
- **GIVEN** an offset category with no pillar
- **WHEN** a reimbursement offsets it
- **THEN** the reduction SHALL apply to `need`.

### Requirement: Excess Reimbursement
When reimbursements naming X exceed X's reportable expenses in the window, X's spending and the reduction of its pillar SHALL floor at zero. The remainder SHALL be returned as `reimbursement_excess`; it SHALL count neither as income nor as a reduction of any other category, and SHALL NOT carry into another window.

#### Scenario: Repayment larger than the spending in the window
- **GIVEN** a cycle with salary Rp 5.000.000, Rp 100.000 spent in "Makan Bareng" and a Rp 150.000 reimbursement offsetting it
- **WHEN** the dashboard overview is requested
- **THEN** "Makan Bareng" SHALL show `spent` 0 and `want_spent` SHALL be 0
- **AND** `reimbursement_excess` SHALL be Rp 50.000
- **AND** `total_inflow` SHALL be Rp 5.000.000.

### Requirement: Reimbursements in Cycle Spending and Safe-to-Spend
Pulse cycle spending SHALL be reduced by the applied offsets of categories whose pillar is not saving, floored at zero, and safe-to-spend SHALL use the reduced figure. Pulse SHALL return the reduction as `cycle_reimbursed`. Today's spending, daily series, heatmaps and burn rates SHALL remain gross spending.

#### Scenario: Repayment restores safe-to-spend
- **GIVEN** a monthly budget of Rp 3.000.000 and the cycle of the "Shared dinner partly repaid" scenario
- **WHEN** pulse is requested with R days remaining in the cycle
- **THEN** `cycle_spent` SHALL be Rp 300.000 and `cycle_reimbursed` Rp 200.000
- **AND** `safe_to_spend_today` SHALL be Rp 2.700.000 divided by R, rounded down.

### Requirement: Choosing the Offset Category in Forms
Quick capture and the ledger edit dialog SHALL show an expense-category select labelled "Mengurangi pengeluaran" when the chosen income category offsets spending, SHALL list only active budget-reportable expense categories, SHALL allow leaving it empty, and SHALL send the choice as `offset_category_id`.

#### Scenario: Quick capture of a repayment
- **WHEN** the owner chooses income and the category "Patungan & Ganti Uang"
- **THEN** the "Mengurangi pengeluaran" select SHALL appear and be reachable by keyboard
- **AND** submitting SHALL send the chosen expense category as `offset_category_id`.

#### Scenario: Ordinary income hides the select
- **WHEN** the owner chooses the income category "Gaji"
- **THEN** no offset select SHALL be shown and no offset SHALL be sent.
