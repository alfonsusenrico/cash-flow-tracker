# Spec Delta

## ADDED Requirements

### Requirement: Reconciliation-Required Account State
`GET /api/accounts` SHALL expose for each account whether it requires reconciliation (`reconciliation_required`), the reason, the notification event that caused it and the tracked discrepancy, without exposing notification payload text. Completing a successful balance reconciliation SHALL clear the state in the same database transaction as the balance-adjustment transaction.

#### Scenario: Clearing a notification discrepancy
- **WHEN** a user reconciles an account that was flagged after a settled notification exceeded its tracked balance
- **THEN** the adjustment transaction commits and the reconciliation-required state is cleared together
