## MODIFIED Requirements

### Requirement: Reimbursements Are Not Income
Every income total SHALL exclude reimbursements: dashboard overview and analytics totals and comparisons, daily series, pulse cycle income, the insights totals and the ledger summary inflow. Unlinked reimbursements without an offset category SHALL reduce the window's total `Pengeluaran` and `Belanja` (floored at zero, overflow reported in `reimbursement_excess`), while the uncounted remainder of an explicitly linked reimbursement SHALL stay in `reimbursement_unallocated` and change no spending figure.

#### Scenario: Unallocated reimbursement
- **GIVEN** a cycle with salary Rp 5.000.000, Rp 500.000 of reportable expenses, and a Rp 80.000 unlinked reimbursement without an offset category
- **WHEN** the dashboard overview is requested
- **THEN** `total_inflow` SHALL be Rp 5.000.000
- **AND** `total_outflow` SHALL be Rp 420.000 with `reimbursement_offset` Rp 80.000 and `reimbursement_unallocated` 0
- **AND** individual category and pillar totals SHALL remain Rp 500.000

#### Scenario: Unlinked reimbursement exceeding period spending floors at zero
- **GIVEN** a cycle with Rp 100.000 of reportable expenses and a Rp 150.000 unlinked reimbursement without an offset category
- **WHEN** the dashboard overview is requested
- **THEN** `total_outflow` SHALL be Rp 0, `reimbursement_offset` Rp 100.000, and `reimbursement_excess` Rp 50.000

### Requirement: Reimbursements Reduce Spending of Their Offset Category
For a report window, the applied offset of category X SHALL be the smaller of X's reportable expenses and reimbursements naming X in the window. Total spending and X's category spending SHALL be reduced by that amount. The applied amount SHALL be allocated proportionally among the effective pillars of X's expenses with exact integer totals and deterministic rounding.

#### Scenario: Shared dinner partly repaid
- **GIVEN** salary Rp 5.000.000, Rp 300.000 spent in a want category, Rp 200.000 in a need category, a Rp 200.000 reimbursement offsetting the want category and Rp 80.000 unlinked reimbursement without an offset category in the same window
- **WHEN** dashboard overview is requested
- **THEN** inflow SHALL be Rp 5.000.000, gross outflow Rp 500.000, applied offset Rp 280.000, net outflow Rp 220.000, want spending Rp 100.000, and need spending Rp 200.000

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
- **THEN** its effective pillar SHALL remain `unclassified`

## ADDED Requirements

### Requirement: Exact Single-Match Auto-Linking for Reimbursements
When a reimbursement is recorded or recategorized into an offsetting category with no explicit links and no `offset_category_id`, the backend SHALL auto-link it to an unlinked reportable expense of the same owner within the preceding 14 days if and only if **exactly one** such expense has the exact same amount. Ambiguous, split, or rounded amounts SHALL NOT be auto-linked, and manually unlinking SHALL NOT re-link.

#### Scenario: Exact single match within 14 days auto-links
- **GIVEN** one unlinked Rp 800.000 expense recorded 4 hours earlier
- **WHEN** a Rp 800.000 reimbursement arrives in "Patungan & Ganti Uang" with no offset category
- **THEN** the backend SHALL create a Rp 800.000 link in `reimbursement_links` to that expense, lowering its category and pillar to Rp 0

#### Scenario: Split or rounded repayment is not auto-linked
- **GIVEN** a Rp 198.800 expense recorded 1 day earlier
- **WHEN** a Rp 50.000 rounded split reimbursement arrives in "Patungan & Ganti Uang"
- **THEN** no link SHALL be created and the Rp 50.000 SHALL reduce period total spending as an unlinked reimbursement

#### Scenario: Two matching expenses are ambiguous and stay unlinked
- **GIVEN** two unlinked Rp 100.000 expenses recorded within the last 14 days
- **WHEN** a Rp 100.000 reimbursement arrives in "Patungan & Ganti Uang"
- **THEN** no link SHALL be created
