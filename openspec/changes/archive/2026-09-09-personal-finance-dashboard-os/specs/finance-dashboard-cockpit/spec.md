## ADDED Requirements

### Requirement: Executive KPI Summary Ribbon
The dashboard SHALL display an executive summary ribbon containing:
1. `Liquid Net Worth`: Total liquid balance across all active accounts.
2. `Total Inflow`: Income recorded within the active timeframe.
3. `Total Outflow`: Expenses recorded within the active timeframe.
4. `Net Savings Rate`: Percentage of inflow retained as savings `((Inflow - Outflow) / max(1, Inflow)) * 100`.
5. `Cash Runway`: Estimated days of runway based on liquid balance divided by average daily burn rate.

#### Scenario: Displaying executive KPIs
- **WHEN** the user views the Overview dashboard
- **THEN** all 5 KPI cards render with tabular formatted currency, percentage trends, and status coloring

### Requirement: Cumulative Cash Flow Area Trendline
The dashboard SHALL render an interactive Recharts Area/Line chart visualizing cumulative cash flow across the selected timeframe:
1. An emerald green area curve for cumulative inflow.
2. A coral/rose area curve for cumulative outflow.
3. Hovering over any point SHALL display a detailed tooltip with the date, cumulative values, and net variance.

#### Scenario: Inspecting cumulative trendline point
- **WHEN** the user hovers over a date on the cumulative cash flow chart
- **THEN** the tooltip displays the exact cumulative income, cumulative expense, and net spread on that date

### Requirement: Burn Cadence Bar Chart with Allowance Benchmark
The dashboard SHALL render an interactive Bar chart displaying daily spending throughout the cycle, with a dashed reference line representing the daily safe-to-spend allowance budget ceiling.

#### Scenario: Highlighting over-budget days
- **WHEN** a day's spending exceeds the safe-to-spend allowance
- **THEN** the bar displays with high-contrast attention color above the benchmark reference line

### Requirement: Category Spend Donut & Variance Matrix
The dashboard SHALL render an interactive Donut chart displaying the proportion of spending by category, coupled with a ranked variance list showing actual spend versus monthly budget ceiling.

#### Scenario: Selecting category slice
- **WHEN** the user hovers over or taps a donut slice
- **THEN** the center label highlights the category name, total amount, and percentage share
