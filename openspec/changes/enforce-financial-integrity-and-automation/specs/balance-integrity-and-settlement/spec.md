# Spec Delta

## Purpose

Defines the balance, locking, and atomic-settlement guarantees that keep every user-visible financial mutation internally consistent.

## ADDED Requirements

### Requirement: Non-Negative Liquid Source Balances
The system SHALL reject a user-initiated expense, internal movement, investment purchase, recurring execution, or payroll allocation when the resolved liquid source account lacks the required available balance. The rejection SHALL identify the required and available amounts and SHALL leave all related records unchanged.

#### Scenario: Rejecting an unaffordable transfer
- **WHEN** a user attempts to move 600,000 IDR from a liquid account with 500,000 IDR available
- **THEN** the system rejects the movement with an insufficient-funds response containing both amounts
- **AND** neither half of the movement is created

### Requirement: Investment Positions Are Trade-Only
The system SHALL allow investment-position balances and units to change only through a validated investment trade that includes units and price. Generic internal movements, recurring transfers, and payroll allocations SHALL reject investment positions as either source or destination. Ordinary liquid investment-funding accounts remain eligible for these operations.

#### Scenario: Rejecting a generic movement into an investment position
- **WHEN** a user attempts to move money directly from a liquid account into an investment position through the generic movement endpoint
- **THEN** the system rejects the request with an actionable trade-required error
- **AND** neither movement record is created and the position is unchanged

#### Scenario: Rejecting a recurring or payroll transfer to an investment position
- **WHEN** a user creates or executes a recurring or payroll allocation whose source or destination is an investment position
- **THEN** the system rejects the rule or entire batch without ledger or schedule changes

#### Scenario: Funding an investment through a valid trade
- **WHEN** a user buys an investment using an active liquid funding account and an owned investment position
- **THEN** the trade settles cash and units atomically through the investment trade flow

### Requirement: Atomic Financial Settlement
The system SHALL lock and validate every affected balance or position before mutation, and SHALL commit all ledger rows, position changes, goal or obligation effects, and scheduling state for one financial operation in a single database transaction.

#### Scenario: Rolling back a multi-record operation
- **WHEN** any write in an investment trade, bilateral movement, payroll batch, or recurring execution fails
- **THEN** every write belonging to that operation is rolled back
- **AND** balances, positions, linked entities, and schedule state remain unchanged

### Requirement: Settled External Event Exception
The system SHALL record an authenticated, supported, settled notification even when the tracked liquid balance is insufficient, and SHALL mark the affected account as requiring reconciliation instead of discarding the real-world event.

#### Scenario: Recording a settled event against a stale balance
- **WHEN** a supported bank notification reports a settled 200,000 IDR expense but the tracked account contains 150,000 IDR
- **THEN** the transaction is recorded exactly once
- **AND** the account is marked for reconciliation with its resulting discrepancy visible to the user
