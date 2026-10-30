# Spec Delta

## ADDED Requirements

### Requirement: Concurrency-Safe Notification Application
The system SHALL claim each `(user, payload_hash)` event before applying financial effects and SHALL associate that event with at most one logical ledger operation. Concurrent or repeated deliveries SHALL return the existing result without creating additional transactions, positions, pockets, or account effects.

#### Scenario: Receiving the same notification concurrently
- **WHEN** two authenticated requests submit the same user's payload hash at the same time
- **THEN** exactly one notification event and one corresponding logical ledger operation are committed
