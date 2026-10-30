# salary-cycles Specification

## Purpose
Use confirmed actual salary receipts to define navigable financial cycles.

## Requirements

### Requirement: Confirm salary sources and receipts
The system SHALL let the owner identify a salary source and confirm or correct qualifying salary income without creating an additional transaction.

#### Scenario: Bonus or refund
- **WHEN** incoming income is a bonus, refund or otherwise ambiguous
- **THEN** it SHALL NOT automatically create a salary cycle merely because it is large or from a similar source

#### Scenario: Explicit salary from confirmed source
- **WHEN** an eligible recorded income matches the confirmed source and explicitly says `gaji` or `salary` without a bonus or refund indicator
- **THEN** the system SHALL automatically confirm it as a salary receipt without creating another ledger transaction

#### Scenario: Unclear purpose from confirmed employer
- **WHEN** a payment from the confirmed employer cannot be distinguished as salary or bonus
- **THEN** the system SHALL ask the owner to confirm its purpose before it creates a cycle boundary

#### Scenario: Correct a candidate
- **WHEN** the owner changes the classification or date of a salary receipt
- **THEN** affected cycle boundaries and reports SHALL be recomputed without duplicating or deleting ledger entries

### Requirement: Observed salary cycle boundaries
The system SHALL begin a cycle at a qualifying confirmed salary receipt and close it at the next confirmed receipt.

#### Scenario: Payday is late
- **WHEN** no next salary receipt has been confirmed
- **THEN** the active cycle SHALL remain open and its future end SHALL be unknown

#### Scenario: Missing historical salary
- **WHEN** a period has no confirmed salary boundary
- **THEN** the system SHALL disclose the gap and offer calendar-period reporting instead of inventing a fixed payday

#### Scenario: Multiple salary candidates
- **WHEN** multiple candidate receipts might represent the same payday
- **THEN** the system SHALL request review and avoid creating duplicate boundaries
