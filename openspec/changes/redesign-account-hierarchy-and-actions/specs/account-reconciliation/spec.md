# Spec Delta

## MODIFIED Requirements

### Requirement: 1-Tap Account Balance Reconciliation
The application SHALL provide a `Sesuaikan Saldo` balance reconciliation action for every eligible account or pocket in `/accounts`, reachable from that item's labelled actions:
1. The modal SHALL display the current tracked balance and provide an input for the user's actual real-world balance.
2. The system SHALL display the calculated variance (e.g. `+15,000 IDR` or `-8,500 IDR`).
3. On confirmation, the backend SHALL update the account balance to the actual amount and generate an audit transaction labelled "Balance Adjustment" or with the user's custom notes.
4. The audit transaction SHALL use the user's system category "Penyesuaian Saldo" of the matching kind (income or expense). That category SHALL be excluded from budget, Kakeibo, spending and income reports, and SHALL carry no Kakeibo pillar. The system SHALL create the category when it does not exist.
5. The user MAY recategorise an adjustment to an ordinary category when it represents real spending or income.

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
