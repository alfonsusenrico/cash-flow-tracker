# Spec Delta

## MODIFIED Requirements

### Requirement: 1-Tap Payroll Allocation Modal & Execution
The application SHALL provide a reviewable payroll allocation batch and execute selected items as one atomic operation:
1. The modal SHALL display all explicitly configured payroll allocations and allow amount adjustment or deselection.
2. Every selected item SHALL use distinct, active, user-owned liquid source and target accounts; investment positions SHALL be rejected as either endpoint.
3. The server SHALL lock affected sources and validate the aggregate amount drawn from each source before writing any item.
4. Each allocation SHALL create a durably linked bilateral movement using the canonical movement contract.
5. If any item is invalid or unaffordable, the entire batch SHALL fail without ledger, balance, or schedule changes.
6. Successful execution SHALL advance only the rules represented by committed items and SHALL return a per-item success summary.

#### Scenario: Executing an affordable payroll allocation batch
- **WHEN** a user confirms four valid allocations whose combined source requirement is available
- **THEN** all four linked movements commit atomically and their rule dates advance once

#### Scenario: Rejecting an unaffordable payroll batch
- **WHEN** one source lacks the aggregate balance required by its selected allocations
- **THEN** the entire batch is rejected and no allocation or rule date changes

#### Scenario: Executing monthly payroll allocation in 1 tap
- **WHEN** a user confirms a valid 10,000,000 IDR split across four destinations
- **THEN** all four linked movements commit atomically, affected balances update, and represented rule dates advance once

#### Scenario: Rejecting a payroll allocation to an investment position
- **WHEN** a selected payroll allocation names an investment position as its source or destination
- **THEN** the entire batch is rejected without creating movements or advancing rule dates
