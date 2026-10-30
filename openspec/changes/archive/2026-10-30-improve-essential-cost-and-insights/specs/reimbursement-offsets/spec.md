## MODIFIED Requirements

### Requirement: Reimbursements in Cycle Spending and Safe-to-Spend
The system SHALL apply eligible reimbursements to observed cycle spending according to its reporting scope, without deriving a daily allowance from an assumed cycle end. The essential monthly cost SHALL use each period's essential spending net of the `need` share of applied reimbursements, as defined by `emergency-cost-estimation`.

#### Scenario: Repayment restores safe-to-spend
- **WHEN** a cycle contains reportable spending of Rp 500.000 and an applicable Rp 200.000 offset
- **THEN** cycle spending SHALL report Rp 300.000 net and disclose the Rp 200.000 offset
- **AND** it SHALL NOT infer a daily allowance from a guessed payday

#### Scenario: Essential cost is net of the need share
- **WHEN** a completed period has Rp 100.000 of `need` and Rp 300.000 of `want` spending in one category and a Rp 200.000 reimbursement offsetting it
- **THEN** that period's essential spending SHALL be reduced by Rp 50.000
