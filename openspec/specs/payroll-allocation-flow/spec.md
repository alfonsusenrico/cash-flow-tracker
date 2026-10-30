# payroll-allocation-flow Specification

## Purpose
Configure payroll allocation bundles and execute the chosen allocations through explicit user confirmation.

## Requirements

### Requirement: Retired payroll allocation stays non-posting
The system SHALL NOT post payroll allocation batches. Rules that were flagged as payroll allocations SHALL remain as non-posting plans with their history, and the payroll flag SHALL only be clearable, never newly set.

#### Scenario: Old client requests payroll execution
- **WHEN** a client calls `POST /api/recurring/payroll/execute`
- **THEN** the response SHALL be 410 with code `payroll_execution_retired`, and no transaction SHALL be created

#### Scenario: Flagging a rule as payroll allocation
- **WHEN** a client creates or updates a plan and sends `is_payroll_allocation` set to true
- **THEN** the request SHALL be rejected with 422 and code `payroll_allocation_retired`, and the plan SHALL be unchanged

#### Scenario: Existing payroll-flagged rule after migration
- **WHEN** the owner views a plan that was a payroll allocation before the migration
- **THEN** it SHALL appear as a non-posting plan, and its earlier occurrences and recorded transfers SHALL be unchanged
