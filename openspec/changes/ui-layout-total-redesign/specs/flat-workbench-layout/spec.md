# Spec: flat-workbench-layout

## Overview
All four core screens SHALL use a flat, zero-elevation workbench layout inside the app window.

## Requirements

- The main content area of Beranda, Ledger, Insights, and Accounts SHALL NOT use white elevated
  cards (`card-crisp`) for primary content sections.
- Primary content sections SHALL be separated by 1px `var(--border-divider)` horizontal dividers.
- The workbench SHALL use transparent background, inheriting the `--canvas-app` of the window.
- Outer workbench padding SHALL be 0; inner section padding SHALL be 16px vertical, 24px horizontal.
- On desktop (`≥1024px`), the workbench SHALL use a 2-column split: left (flex-1) and right
  (fixed 320px), separated by a 1px `var(--border-structural)` vertical divider.
- On tablet/mobile, the 2-column split SHALL collapse to a single stacked column.

## Scenarios

#### Scenario: Beranda workbench uses flat sections
- **GIVEN** the user is on the Beranda page
- **WHEN** the dashboard data loads
- **THEN** sections (KPI strip, kakeibo track, activity feed, accounts vault) SHALL render
  without white card backgrounds or box shadows
- **AND** sections SHALL be separated only by 1px bottom borders

#### Scenario: Ledger table fills height
- **GIVEN** the user is on the Ledger page
- **WHEN** there are transactions to display
- **THEN** the data table SHALL fill the available vertical height of the workbench
- **AND** there SHALL be no page-level `<h1>` heading inside the workbench content area

#### Scenario: Insights uses 2-column workbench
- **GIVEN** the user is on the Insights page on a desktop viewport
- **WHEN** analytics data loads
- **THEN** the daily cadence bar chart SHALL appear in the left 60% column
- **AND** category benchmark cards SHALL appear in the right 40% column, scrollable independently
