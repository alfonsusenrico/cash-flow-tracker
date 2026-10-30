# account-reconciliation Specification

## Purpose
Allow users to reconcile tracked account balances with an actual balance and retain an auditable adjustment.

## Requirements

### Requirement: 1-Tap Account Balance Reconciliation
The application SHALL provide a `Sesuaikan Saldo` balance reconciliation action for every eligible account or pocket in `/accounts`, reachable from that item's labelled actions.

#### Scenario: Reconciling an account balance discrepancy
- **WHEN** the user inputs an actual balance of 10,000,000 IDR for an account currently tracking at 9,950,000 IDR
- **THEN** the backend updates the account balance to 10,000,000 IDR and creates an income adjustment transaction of 50,000 IDR dated today
- **AND** the adjustment SHALL belong to the income category "Penyesuaian Saldo".

#### Scenario: Reconciling negative balance discrepancy
- **WHEN** the user inputs an actual balance of 9,900,000 IDR for an account currently tracking at 9,950,000 IDR
- **THEN** the backend updates the account balance to 9,900,000 IDR and creates an expense adjustment transaction of 50,000 IDR labeled "Balance Adjustment"
- **AND** the adjustment SHALL belong to the expense category "Penyesuaian Saldo".

#### Scenario: Adjustments do not distort reports
- **GIVEN** an expense adjustment of 50,000 IDR in the current cycle
- **WHEN** the dashboard computes spending, Kakeibo allocation and safe-to-spend
- **THEN** the adjustment SHALL NOT be counted in any of them
- **AND** the account balance SHALL still reflect it.

#### Scenario: Existing uncategorised adjustments are migrated
- **GIVEN** a stored transaction with notes "Balance Adjustment (Reconciliation)", no category, no movement and no idempotency key
- **WHEN** the schema migration runs
- **THEN** the transaction SHALL move to the "Penyesuaian Saldo" category of its kind
- **AND** transactions with any other notes or an existing category SHALL NOT change.

#### Scenario: Required behavior and constraints
- **WHEN** an owner reconciles the recorded balance of an eligible account or pocket
- **THEN** the following required behavior and constraints hold:

  The application SHALL provide a `Sesuaikan Saldo` balance reconciliation action for every eligible account or pocket in `/accounts`, reachable from that item's labelled actions:
  1. The modal SHALL display the current tracked balance and provide an input for the user's actual real-world balance.
  2. The system SHALL display the calculated variance (e.g. `+15,000 IDR` or `-8,500 IDR`).
  3. Upon confirmation, the backend SHALL update the account balance to the actual amount and automatically generate an audit transaction labeled "Balance Adjustment" or custom user notes.

#### Scenario: Finding reconciliation in the redesigned hierarchy
- **WHEN** a user opens the actions for an eligible account or pocket on desktop or mobile
- **THEN** a clearly labelled `Sesuaikan Saldo` action opens the existing reconciliation flow for that exact item

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

### Requirement: Reconciliation-Required Account State
`GET /api/accounts` SHALL expose for each account whether it requires reconciliation (`reconciliation_required`), the reason, the notification event that caused it and the tracked discrepancy, without exposing notification payload text. Completing a successful balance reconciliation SHALL clear the state in the same database transaction as the balance-adjustment transaction.

#### Scenario: Clearing a notification discrepancy
- **WHEN** a user reconciles an account that was flagged after a settled notification exceeded its tracked balance
- **THEN** the adjustment transaction commits and the reconciliation-required state is cleared together
