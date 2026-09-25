# Spec Delta

## ADDED Requirements

### Requirement: Concurrency-Safe Notification Application
The system SHALL claim each `(user, payload_hash)` event before applying financial effects and SHALL associate that event with at most one logical ledger operation. Concurrent or repeated deliveries SHALL return the existing result without creating additional transactions, positions, pockets, or account effects.

#### Scenario: Receiving the same notification concurrently
- **WHEN** two authenticated requests submit the same user's payload hash at the same time
- **THEN** exactly one notification event and one corresponding logical ledger operation are committed

### Requirement: Notification Reconciliation State
When a supported settled event causes a tracked liquid account to fall below zero, ingestion SHALL retain the event, mark the account for reconciliation, and expose the discrepancy without treating the event as ordinary available negative funds.

#### Scenario: Settled expense exceeds tracked balance
- **WHEN** ingestion applies a settled expense larger than the tracked source balance
- **THEN** the event remains recorded and the account is marked as requiring reconciliation
