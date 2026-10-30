# account-reconciliation Specification

## Purpose
Allow users to reconcile tracked account balances with an actual balance and retain an auditable adjustment.

## Requirements

### Requirement: 1-Tap Account Balance Reconciliation
The application SHALL provide a balance reconciliation action on every account card in `/accounts`.

#### Scenario: Reconciling an account balance discrepancy
- **WHEN** an actual balance exceeds the tracked balance by 50,000 IDR
- **THEN** the backend SHALL update the account balance and create an income adjustment of 50,000 IDR dated today in the income category `Penyesuaian Saldo`

#### Scenario: Reconciling negative balance discrepancy
- **WHEN** an actual balance is 50,000 IDR below the tracked balance
- **THEN** the backend SHALL update the account balance and create a 50,000 IDR expense adjustment in the expense category `Penyesuaian Saldo`

#### Scenario: Adjustments do not distort reports
- **WHEN** the dashboard calculates spending or Kakeibo
- **THEN** reconciliation adjustments SHALL be excluded while account balance reflects them

#### Scenario: Existing uncategorised adjustments are migrated
- **GIVEN** a stored transaction with notes `Balance Adjustment (Reconciliation)`, no category, no movement and no idempotency key
- **WHEN** the schema migration runs
- **THEN** the transaction SHALL move to the `Penyesuaian Saldo` category of its kind
- **AND** transactions with other notes or an existing category SHALL not change

### Requirement: Reconciliation Variance Preview
The reconciliation modal SHALL display the current tracked balance, provide an input for the user's actual real-world balance, and display the calculated variance (e.g. `+15,000 IDR` or `-8,500 IDR`).

#### Scenario: Variance shown before confirmation
- **GIVEN** an account tracking 9,950,000 IDR
- **WHEN** the user types an actual balance of 9,965,000 IDR in the reconciliation modal
- **THEN** the modal SHALL show the tracked balance and a variance of `+15,000 IDR` before anything is saved.

### Requirement: Reconciliation Audit Transaction
On confirmation, the backend SHALL update the account balance to the actual amount and generate an audit transaction labelled "Balance Adjustment" or with the user's custom notes.

#### Scenario: Custom notes label the adjustment
- **WHEN** the user confirms a reconciliation with the notes "Selisih bunga"
- **THEN** the account balance SHALL equal the actual amount
- **AND** the audit transaction SHALL carry the notes "Selisih bunga".

### Requirement: Penyesuaian Saldo System Category
The audit transaction SHALL use the user's system category "Penyesuaian Saldo" of the matching kind (income or expense). That category SHALL be excluded from budget, Kakeibo, spending and income reports, and SHALL carry no Kakeibo pillar. The system SHALL create the category when it does not exist.

#### Scenario: Missing adjustment category is created
- **GIVEN** a user without an income "Penyesuaian Saldo" category
- **WHEN** the user reconciles an account upwards
- **THEN** the system SHALL create the income category "Penyesuaian Saldo", excluded from budget and without a pillar, and file the adjustment under it.

### Requirement: Recategorising an Adjustment
The system SHALL let the user recategorise an adjustment to an ordinary category when it represents real spending or income.

#### Scenario: Adjustment that was real spending
- **GIVEN** an expense adjustment of 50,000 IDR in "Penyesuaian Saldo"
- **WHEN** the user moves it to "Makanan & Minuman"
- **THEN** it SHALL count as spending in that category like any other expense.
