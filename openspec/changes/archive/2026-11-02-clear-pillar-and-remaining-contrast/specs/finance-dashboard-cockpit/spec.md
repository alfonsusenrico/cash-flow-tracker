## MODIFIED Requirements

### Requirement: Category Spend Donut & Variance Matrix
The dashboard SHALL render an interactive Donut chart displaying the proportion of spending by category, coupled with a ranked variance list showing actual spend versus monthly budget ceiling. The donut SHALL be a named figure with a text summary of every category it draws, and each slice SHALL carry its category, amount and share as its accessible name.

#### Scenario: Selecting category slice
- **WHEN** the user hovers over or taps a donut slice
- **THEN** the center label highlights the category name, total amount, and percentage share

#### Scenario: Donut for assistive technology
- **WHEN** Beranda draws the donut for five categories at 1280 px, in the light or the dark theme
- **THEN** the figure SHALL be named "Diagram donat" followed by the card title, its description SHALL list the total and each category with amount and share, every slice SHALL have an accessible name, and axe SHALL report no `svg-img-alt`
