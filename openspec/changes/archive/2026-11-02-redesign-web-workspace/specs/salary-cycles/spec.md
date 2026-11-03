# Spec Delta

## ADDED Requirements

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
