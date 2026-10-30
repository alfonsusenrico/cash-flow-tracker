## ADDED Requirements

### Requirement: Five classifications visible and traceable
The system SHALL present need, want, culture, unexpected and saving individually with their amount, basis, and relevant ledger drill-down.

#### Scenario: Review unexpected spending
- **WHEN** a user views the breakdown
- **THEN** unexpected spending SHALL be distinguishable from need even if a grouped benchmark is also shown

#### Scenario: Reimbursement of mixed-classification spending
- **WHEN** a category contains expenses in multiple effective classifications and receives a reimbursement without an individual expense link
- **THEN** the applied category offset SHALL be distributed proportionally across those classifications with exact integer totals and deterministic rounding
- **AND** net expense classifications before adding savings movements SHALL reconcile to net reportable spending

### Requirement: Explain savings and benchmark
The system SHALL distinguish net cash-flow surplus from fresh savings and explain any optional 50/30/20 mapping without presenting the benchmark as a health verdict.

#### Scenario: Transfer and investment trade
- **WHEN** an operational transfer, a qualifying fresh-saving movement and an investment trade occur
- **THEN** the savings figure SHALL identify the qualifying contribution once and SHALL NOT count ordinary transfers or trade turnover as new savings

#### Scenario: Explain the combined savings figure
- **WHEN** the savings classification includes reportable saving-classified expenses and qualifying savings movements
- **THEN** the response SHALL show those components separately and explain the combined figure, while retaining the legacy combined field

## MODIFIED Requirements

### Requirement: Normalized 50/30/20 Allocation Proportions
The system SHALL calculate the five pillar amounts and percentages using `base = max(total_inflow, total_allocated)`, where total allocated is the sum of the five amounts and fresh saving is floored at zero.

#### Scenario: Allocation percentages never exceed 100%
- **WHEN** a Kakeibo breakdown has a positive base
- **THEN** each pillar percentage SHALL use that base and their total SHALL be at most 100% within rounding

#### Scenario: Allocation percentages always sum to 100%
- **WHEN** total allocated is positive and at least total inflow
- **THEN** the five percentages SHALL sum to 100% within rounding

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

### Requirement: 50/30/20 Group Benchmark
The system SHALL explain an optional 50/30/20 comparison using essentials = need + unexpected, lifestyle = want + culture and saving.

#### Scenario: Group percentages
- **WHEN** need is Rp 300.000, unexpected Rp 100.000, want Rp 150.000, culture Rp 50.000 and saving Rp 400.000 against a Rp 1.000.000 base
- **THEN** essentials SHALL be 40% and lifestyle SHALL be 20%

### Requirement: Stable Kakeibo Response Fields
The system SHALL preserve the legacy Kakeibo response fields and their documented meanings on the compatibility contract until clients are migrated, while the updated interface avoids financial-health verdicts.

#### Scenario: Existing fields keep their meaning
- **WHEN** an older client reads the legacy response
- **THEN** its need, want, saving and legacy status fields SHALL remain present with their legacy semantics

## REMOVED Requirements

### Requirement: Kakeibo Status
**Reason**: A fixed benchmark should not present a financial-health verdict.
**Migration**: Preserve versioned compatibility for legacy response fields while replacing the visible verdict with explained figures.
