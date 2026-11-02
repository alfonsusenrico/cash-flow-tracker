## MODIFIED Requirements

### Requirement: Normalized 50/30/20 Allocation Proportions
The system SHALL calculate the five pillar amounts, the unclassified amount and their percentages using `base = max(total_inflow, total_allocated)`, where total allocated is the sum of the five pillar amounts and the unclassified amount and fresh saving is floored at zero.

#### Scenario: Allocation percentages never exceed 100%
- **WHEN** a Kakeibo breakdown has a positive base
- **THEN** each pillar percentage and the unclassified percentage SHALL use that base and their total SHALL be at most 100% within rounding

#### Scenario: Allocation percentages always sum to 100%
- **WHEN** total allocated is positive and at least total inflow
- **THEN** the five pillar percentages and the unclassified percentage SHALL sum to 100% within rounding

#### Scenario: Unallocated inflow stays unallocated
- **WHEN** income is Rp 2.000.000, need is Rp 500.000 and the other allocations are zero
- **THEN** need SHALL be 25% and the remainder SHALL not be assigned to a pillar

#### Scenario: Lifestyle group over target
- **WHEN** want and culture sum to Rp 400.000 on a base of Rp 1.000.000
- **THEN** lifestyle SHALL be 40% and any optional benchmark SHALL explain the mapping

#### Scenario: Essentials group over target
- **WHEN** need and unexpected sum to Rp 600.000 on a base of Rp 1.000.000
- **THEN** essentials SHALL be 60% and any optional benchmark SHALL explain the mapping

#### Scenario: Healthy allocation is shown as healthy
- **WHEN** the owner views a breakdown that meets the optional benchmark
- **THEN** the interface SHALL present the actual figures without a financial-health verdict

#### Scenario: Empty cycle
- **WHEN** no inflow or allocation is available for the selected period
- **THEN** the interface SHALL show an empty state rather than a healthy verdict

#### Scenario: Unclassified spending stays outside the groups
- **WHEN** Rp 200.000 of spending is unclassified on a base of Rp 1.000.000
- **THEN** the unclassified percentage SHALL be 20%, no group SHALL include it, and any optional benchmark SHALL say that it is left out of the groups

### Requirement: Five classifications visible and traceable
The system SHALL present need, want, culture, unexpected and saving individually with their amount, basis, and relevant ledger drill-down, and SHALL present unclassified spending as "Belum diklasifikasi" in the same way, with a drill-down to its rows and a way to complete the classification.

#### Scenario: Review unexpected spending
- **WHEN** a user views the breakdown
- **THEN** unexpected spending SHALL be distinguishable from need even if a grouped benchmark is also shown

#### Scenario: Reimbursement of mixed-classification spending
- **WHEN** a category contains expenses in multiple effective classifications and receives a reimbursement without an individual expense link
- **THEN** the applied category offset SHALL be distributed proportionally across those classifications with exact integer totals and deterministic rounding
- **AND** net expense classifications before adding savings movements SHALL reconcile to net reportable spending

#### Scenario: Unclassified spending is traceable
- **WHEN** a breakdown holds unclassified spending
- **THEN** it SHALL be shown as "Belum diklasifikasi" with its amount, linked to the ledger filtered to `kakeibo=unclassified`, and the five pillars and it SHALL add up to net reportable spending before savings movements
