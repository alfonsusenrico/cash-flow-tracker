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

### Requirement: Expected Next Salary Date
The system SHALL estimate the next salary date only from the owner's last three confirmed receipts of the confirmed source when they are 26 to 35 days apart and their local days of the month differ by at most 3. The estimate SHALL be the most recent receipt's day in the following month, clamped to that month's length.

#### Scenario: Regular receipts
- **GIVEN** confirmed receipts on 25 August, 25 September and 24 October
- **WHEN** the expected next salary date is computed on 2 November
- **THEN** it SHALL be 24 November with basis `regular_receipts`, naming the three receipts

#### Scenario: Too few receipts
- **GIVEN** only two confirmed receipts
- **WHEN** the expected next salary date is computed
- **THEN** it SHALL be unknown with reason `too_few_receipts`

#### Scenario: Irregular receipts
- **GIVEN** confirmed receipts on 25 August, 10 September and 24 October
- **WHEN** the expected next salary date is computed
- **THEN** it SHALL be unknown with reason `irregular_receipts`

#### Scenario: Expected date has passed
- **GIVEN** an expected date of 24 November and no new confirmed receipt on 26 November
- **WHEN** the estimate is requested
- **THEN** it SHALL report status `overdue` with the expected date, and SHALL NOT move the estimate forward on its own

### Requirement: Expected Date Is Never a Boundary
The expected next salary date SHALL be an estimate for planning only. It SHALL NOT close, shorten or extend a salary cycle, SHALL NOT change any report window, and SHALL NOT be stored as a receipt or boundary.

#### Scenario: Reports keep observed boundaries
- **GIVEN** an expected date of 24 November and an open cycle since 24 October
- **WHEN** the owner opens Analisis or Laporan for the current cycle on 30 November
- **THEN** the cycle SHALL still be open and run to now, and no report SHALL end on 23 November
