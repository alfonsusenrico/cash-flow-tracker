# cycle-end-forecast Specification

## Purpose
TBD - created by archiving change cycle-forecast-burn-rate. Update Purpose after archive.
## Requirements
### Requirement: Active Cycle Outflow Forecasting
The system SHALL compute a linear month-end spending forecast for the active payday cycle based on actual outflow and elapsed days.

#### Scenario: Forecast spending during an active cycle
- **GIVEN** a user is on Day 16 of a 31-day payday cycle (Aug 29 to Sep 28)
- **AND** the user has recorded Rp 50.000 in total expenses during this cycle
- **WHEN** the dashboard overview endpoint `GET /api/dashboard/overview` is requested
- **THEN** `burn_rate.projected_cycle_outflow` SHALL equal `round((50000 / 16) * 31)` which is `96875`
- **AND** `burn_rate.cycle_elapsed_days` SHALL equal `16`
- **AND** `burn_rate.cycle_total_days` SHALL equal `31`
- **AND** `burn_rate.cycle_end_date` SHALL equal `2026-09-28`.

#### Scenario: Zero expenditure in active cycle
- **GIVEN** a user has recorded 0 expense transactions in the active cycle
- **WHEN** the dashboard overview endpoint `GET /api/dashboard/overview` is requested
- **THEN** `burn_rate.projected_cycle_outflow` SHALL equal `0`.

### Requirement: Prevention of Rounding Precision Drift
The system SHALL NOT multiply an already-rounded daily integer rate by 30 to prevent artificial numeric drift.

#### Scenario: Exact 30-day window calculation without rounding drift
- **GIVEN** a user has exactly one expense of Rp 50.000 in the last 30 days
- **WHEN** `burn_rate.projected_30d_outflow` is generated
- **THEN** `burn_rate.projected_30d_outflow` SHALL equal `50000` exactly (not `50010`).

### Requirement: Cycle Forecast Insights KPI Tile
The Insights dashboard UI SHALL display the cycle-aware spending forecast with contextual cycle dates.

#### Scenario: Render cycle forecast card
- **GIVEN** the user views the Insights overview tab
- **WHEN** the burn rate KPI section is rendered
- **THEN** the third tile header SHALL display "Proyeksi Akhir Siklus"
- **AND** the tile value SHALL render the formatted projected cycle outflow
- **AND** the tile subtitle SHALL describe the projection ending on the active cycle's end date with days elapsed.

