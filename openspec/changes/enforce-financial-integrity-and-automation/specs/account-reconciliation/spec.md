# Spec Delta

## ADDED Requirements

### Requirement: Reconciliation-Required Account State
The system SHALL expose whether an account requires reconciliation, the event that caused the state, and the tracked discrepancy without exposing notification payload secrets. Completing a successful balance reconciliation SHALL clear the state atomically with the balance-adjustment transaction.

#### Scenario: Clearing a notification discrepancy
- **WHEN** a user reconciles an account that was flagged after a settled notification exceeded its tracked balance
- **THEN** the adjustment transaction commits and the reconciliation-required state is cleared together
