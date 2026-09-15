## ADDED Requirements

### Requirement: Month-and-Year Stepper Navigation
The primary global navigation bar SHALL provide a month-and-year stepper that explicitly indicates the active calendar month and year (e.g., "September 2026" or "Sep 2026") with left and right chevrons to move backward or forward across discrete monthly statement periods.

#### Scenario: User views active month
- **WHEN** the user opens any screen on the application
- **THEN** the center stepper in the top bar displays the current month and year in Indonesian (e.g. "Sep 2026")

#### Scenario: Navigating to previous months
- **WHEN** the user clicks the left chevron button
- **THEN** the active month shifts backward (e.g. from "Sep 2026" to "Agu 2026"), the dashboard and ledger refresh to display data for that month, and a "Bulan Ini" reset button appears to return immediately to the current month

### Requirement: Removal of Dual-Timeframe Rolling Pills
The application SHALL NOT present dual rolling timeframe selector pills (`[ 30 Hari | 90 Hari ]`) in the global TopBar, unifying all views under monthly statement accounting.

#### Scenario: Clean TopBar presentation
- **WHEN** viewing the TopBar on desktop or mobile
- **THEN** the 30d/90d pill switcher is absent, leaving only the clean monthly stepper, privacy eye, theme switch, and quick capture button
