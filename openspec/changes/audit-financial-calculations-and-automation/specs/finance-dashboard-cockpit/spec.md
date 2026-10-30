## REMOVED Requirements

### Requirement: Burn Cadence Bar Chart with Allowance Benchmark
**Reason**: A daily allowance calculated from a fixed cycle end suggests unsupported precision.
**Migration**: Show descriptive spending and configured budget information without an assumed daily allowance.

### Requirement: Historical Payday Cycle Navigation
**Reason**: Fixed-day navigation cannot represent variable actual salary receipts.
**Migration**: Navigate confirmed salary cycles and available calendar periods with explicit gaps.

## ADDED Requirements

### Requirement: Observed dashboard planning information
The dashboard SHALL display observed period cash flow, configured budget remaining and separately identified upcoming commitments without representing them as guaranteed funds available to spend.

#### Scenario: Unknown next salary date
- **WHEN** the next salary date is unknown
- **THEN** no daily safe-to-spend figure SHALL be inferred from a guessed cycle end

#### Scenario: Upcoming commitments
- **WHEN** active expense plans are due within the next 30 days or overdue
- **THEN** the dashboard SHALL list them with their dates and a total labelled with the 30-day window and SHALL state that plans are reminders, not payments

#### Scenario: Budget remaining
- **WHEN** a monthly budget is configured and the period is a calendar month or salary cycle
- **THEN** the dashboard SHALL show the budget less observed net spending for that period, and SHALL omit it for rolling 30- or 90-day windows
