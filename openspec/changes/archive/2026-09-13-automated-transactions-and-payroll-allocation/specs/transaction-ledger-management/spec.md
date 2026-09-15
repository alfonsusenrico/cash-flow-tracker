## ADDED Requirements

### Requirement: Recurring Rule Origin Tracking in Ledger
The ledger and transaction models SHALL support tracking the originating recurring rule ID (`recurring_rule_id`):
1. Transactions created via automated execution or 1-tap confirmation SHALL store `recurring_rule_id`.
2. The ledger UI SHALL render a subtle recurring badge indicator for transactions originating from recurring rules or payroll allocations.

#### Scenario: Viewing recurring badge on ledger transaction
- **WHEN** user inspects a transaction created by the automated recurring engine
- **THEN** ledger displays a recurring icon badge indicating it was generated from a scheduled rule
