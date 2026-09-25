# Spec Delta

## MODIFIED Requirements

### Requirement: 1-Tap Account Balance Reconciliation
The application SHALL provide a `Sesuaikan Saldo` balance reconciliation action for every eligible account or pocket in `/accounts`, reachable from that item's labelled actions:
1. The modal SHALL display the current tracked balance and provide an input for the user's actual real-world balance.
2. The system SHALL display the calculated variance (e.g. `+15,000 IDR` or `-8,500 IDR`).
3. Upon confirmation, the backend SHALL update the account balance to the actual amount and automatically generate an audit transaction labeled "Balance Adjustment" or custom user notes.

#### Scenario: Reconciling an account balance discrepancy
- **WHEN** the user inputs an actual balance of 10,000,000 IDR for an account currently tracking at 9,950,000 IDR
- **THEN** the backend updates the account balance to 10,000,000 IDR and creates an income adjustment transaction of 50,000 IDR dated today

#### Scenario: Reconciling negative balance discrepancy
- **WHEN** the user inputs an actual balance of 9,900,000 IDR for an account currently tracking at 9,950,000 IDR
- **THEN** the backend updates the account balance to 9,900,000 IDR and creates an expense adjustment transaction of 50,000 IDR labeled "Balance Adjustment"

#### Scenario: Finding reconciliation in the redesigned hierarchy
- **WHEN** a user opens the actions for an eligible account or pocket on desktop or mobile
- **THEN** a clearly labelled `Sesuaikan Saldo` action opens the existing reconciliation flow for that exact item
