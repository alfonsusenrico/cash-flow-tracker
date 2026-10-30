## MODIFIED Requirements

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

### Requirement: Reimbursements in Cycle Spending and Safe-to-Spend
The system SHALL apply eligible reimbursements to observed cycle spending according to its reporting scope, without deriving a daily allowance from an assumed cycle end. The existing emergency-fund calculation SHALL remain unchanged until its deferred review.

#### Scenario: Repayment restores safe-to-spend
- **WHEN** a cycle contains reportable spending of Rp 500.000 and an applicable Rp 200.000 offset
- **THEN** cycle spending SHALL report Rp 300.000 net and disclose the Rp 200.000 offset
- **AND** it SHALL NOT infer a daily allowance from a guessed payday
