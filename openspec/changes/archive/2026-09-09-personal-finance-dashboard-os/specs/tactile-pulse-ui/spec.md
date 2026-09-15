## MODIFIED Requirements

### Requirement: 3-Screen Information Architecture
The web application SHALL organize user navigation into an **Executive Overview + Deep Dive Tabs** dashboard structure:
1. `/` (Overview): Executive financial cockpit featuring top KPI metrics ribbon (Liquid Net Worth, Inflow, Outflow, Savings Rate, Runway), interactive Cumulative Cash Flow Area Trendline, daily cadence bar with budget reference line, category spend donut, and rapid ledger feed.
2. `/insights` (Spending Analytics): In-depth spending analytics featuring day-by-day burn cadence bars, category budget variance matrix, and day-of-week spending density heatmap.
3. `/accounts` (Accounts & Net Worth): Liquid account distribution, obligation liabilities, 1-tap account transfer, and net worth trajectory.
4. `/goals` (Goals & Debts): Interactive savings goals with milestone progress bars and debt/obligation payoff progress meters.

#### Scenario: Navigating between executive dashboard tabs
- **WHEN** a user selects a tab from the executive header navigation
- **THEN** the dashboard switches views instantly without full page reloads and maintains active timeframe filters

#### Scenario: Mobile responsive dashboard navigation
- **WHEN** a user opens the dashboard on a mobile viewport
- **THEN** the navigation collapses to a responsive top tab switcher and bottom action bar, and multi-column visual charts scale to fit the viewport smoothly
