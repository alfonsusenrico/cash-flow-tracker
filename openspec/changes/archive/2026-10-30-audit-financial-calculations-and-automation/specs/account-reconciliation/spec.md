## MODIFIED Requirements

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
