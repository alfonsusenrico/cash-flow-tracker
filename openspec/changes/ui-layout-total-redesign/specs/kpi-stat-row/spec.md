# Spec: kpi-stat-row

## Overview
KPI metrics on Beranda and Insights SHALL render as a unified horizontal strip with `border-r`
column dividers, not as individual elevated card boxes.

## Requirements

- KPI metrics SHALL render in a single row grid with 4 cells (Beranda) or 3 cells (Insights).
- Cells SHALL be separated by `1px solid var(--border-structural)` right borders.
- The strip itself SHALL have a `border-b` bottom border to separate it from the workbench below.
- Each cell SHALL contain: label (muted, small), large metric value (tabular-nums), status badge.
- There SHALL be no individual card background, border, or shadow per cell.
- On tablet: cells collapse to 2 columns; on mobile: single column.

## Scenarios

#### Scenario: KPI strip renders on Beranda
- **GIVEN** the Beranda dashboard data loads
- **WHEN** kpis object is available
- **THEN** 4 KPI metrics SHALL render in a horizontal strip across the full workbench width
- **AND** each metric cell SHALL have a right-border divider (except the last)
- **AND** no individual cell SHALL have a white background or box shadow

#### Scenario: KPI strip skeleton on load
- **GIVEN** the Beranda data is loading
- **WHEN** the page renders
- **THEN** a shimmer skeleton strip SHALL appear in the KPI row position
