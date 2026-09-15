## ADDED Requirements

### Requirement: Inline Category Budget Editing
The `/insights` Spending Analytics page SHALL allow users to set or update any category's monthly spending limit directly from the Category Variance table:
1. Clicking on a category's budget cell or edit button SHALL open an inline popover or modal.
2. The user can enter a new monthly budget limit (or clear it to "No limit").
3. Saving SHALL call `PATCH /api/categories/{id}` and immediately recalculate variance metrics, budget progress bars, and over-budget counts.

#### Scenario: Setting a category budget limit
- **WHEN** the user clicks "No limit" on the "Food & Dining" category and enters `3000000`
- **THEN** the system sets the category's `monthly_budget` to 3,000,000 IDR and updates the progress bar and remaining variance immediately

### Requirement: Historical Payday Cycle Navigation
The dashboard header and filter controls SHALL support navigating backwards and forwards through historical payday cycles:
1. The user can click previous (`‹`) and next (`›`) cycle chevrons.
2. Navigating cycles SHALL query `/api/dashboard/overview` and `/api/dashboard/analytics` with the corresponding cycle offset.
3. The timeframe label SHALL dynamically update to reflect the specific date range of the active historical cycle (e.g. "Cycle: Jul 25 - Aug 24").

#### Scenario: Viewing the previous pay cycle
- **WHEN** the user clicks the previous cycle button (`‹`)
- **THEN** all dashboard charts, KPIs, and cadence bars update to reflect the cash flow data of the previous cycle window

### Requirement: Bibit-Style Hero Portfolio Action Pills
The Overview dashboard hero area SHALL feature high-visibility action pills inspired by the Bibit dashboard:
1. `+ Record` (`N` quick capture trigger)
2. `⇄ Transfer` (1-tap account transfer trigger)
3. `🎯 New Goal` (savings target creation trigger)
4. Key portfolio figures displaying both nominal values and percentage changes.

#### Scenario: Triggering transfer from hero action pill
- **WHEN** the user taps the `⇄ Transfer` hero action pill on the Overview screen
- **THEN** the transfer modal opens with primary accounts preselected for immediate execution
