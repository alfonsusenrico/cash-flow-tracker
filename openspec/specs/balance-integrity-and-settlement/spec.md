# balance-integrity-and-settlement Specification

## Purpose
Defines the balance, locking, and atomic-settlement guarantees that keep every user-visible financial mutation internally consistent.

## Requirements

### Requirement: Non-Negative Liquid Source Balances
The system SHALL reject a manually entered expense, internal movement, investment purchase or explicit plan record when the resolved liquid source account lacks the required available balance. The rejection SHALL identify the required and available amounts and SHALL leave all related records unchanged. A transaction confirmed from AI text capture is the owner's own statement of a real event and MAY take a liquid account below zero.

#### Scenario: Rejecting an unaffordable transfer
- **WHEN** a user attempts to move 600,000 IDR from a liquid account with 500,000 IDR available
- **THEN** the system rejects the movement with an insufficient-funds response containing both amounts
- **AND** neither half of the movement is created

### Requirement: Investment Positions Are Outside Generic Movements
Generic internal movements and transfer plans SHALL reject an investment position as either source or destination. Unit-tracked positions SHALL change through a validated investment trade with units and price, amount-tracked products through an investment top-up, and either through an explicit valuation update. Ordinary liquid investment-funding accounts remain eligible for generic movements.

#### Scenario: Rejecting a generic movement into an investment position
- **WHEN** a user attempts to move money directly from a liquid account into an investment position through the generic movement endpoint
- **THEN** the system rejects the request with an actionable error
- **AND** neither movement record is created and the position is unchanged

#### Scenario: Funding an investment through a valid trade
- **WHEN** a user buys an investment using an active liquid funding account and an owned investment position
- **THEN** the trade settles cash and units atomically through the investment trade flow

### Requirement: Atomic Financial Settlement
The system SHALL lock and validate every affected balance or position before mutation, and SHALL commit all ledger rows, position changes, goal or obligation effects, and plan occurrence state for one financial operation in a single database transaction.

#### Scenario: Rolling back a multi-record operation
- **WHEN** any write in an investment trade, bilateral movement or explicit plan record fails
- **THEN** every write belonging to that operation is rolled back
- **AND** balances, positions, linked entities, and plan state remain unchanged

### Requirement: Settled External Event Exception
The system SHALL record an authenticated, supported, settled notification even when the tracked liquid balance is insufficient, and SHALL mark the affected account as requiring reconciliation instead of discarding the real-world event.

#### Scenario: Recording a settled event against a stale balance
- **WHEN** a supported bank notification reports a settled 200,000 IDR expense but the tracked account contains 150,000 IDR
- **THEN** the transaction is recorded exactly once
- **AND** the account is marked for reconciliation and `GET /api/accounts` reports its discrepancy
