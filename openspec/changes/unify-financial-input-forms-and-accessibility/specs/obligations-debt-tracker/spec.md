# Spec Delta

## ADDED Requirements

### Requirement: Coherent Obligation Form Validation
The obligation form SHALL collect total amount, remaining amount, optional minimum payment, due date, and notes with field-level validation against the final combined state. It SHALL prevent a negative remaining amount, a remaining amount above total, or an invalid date, and SHALL explain automatic archive or reactivation behavior before submission.

#### Scenario: Correcting an excessive remaining balance
- **WHEN** a user enters a remaining amount greater than the total obligation
- **THEN** submission is prevented, the remaining field receives an associated error, and entered values remain available
