# tactile-pulse-ui Specification

## Purpose
TBD - created by archiving change refactor-clean-cashflow-core. Update Purpose after archive.
## Requirements
### Requirement: 3-Screen Information Architecture
The web application SHALL organize user navigation into a 5-screen executive dashboard structure hosted inside a persistent left-hand sidebar:
1. `/` (Overview): Executive financial cockpit featuring top KPI metrics ribbon (Liquid Net Worth, Inflow, Outflow, Savings Rate, Runway), Bibit-style hero action pills, interactive Cumulative Cash Flow Area Trendline, daily cadence bar with budget reference line, category spend donut, and rapid ledger feed.
2. `/ledger` (Transactions): Comprehensive searchable and filterable ledger table with full transaction detail, editing, and deletion capabilities.
3. `/insights` (Spending Analytics): In-depth spending analytics featuring day-by-day burn cadence bars, category budget variance matrix with inline editing, and day-of-week spending density heatmap.
4. `/accounts` (Accounts & Net Worth): Liquid account distribution, obligation liabilities, 1-tap account transfer, 1-tap account balance reconciliation, and net worth trajectory.
5. `/goals` (Goals & Debts): Interactive savings goals with milestone progress bars and debt/obligation payoff progress meters.

#### Scenario: Navigating between executive dashboard screens
- **WHEN** a user selects any item from the persistent left sidebar navigation
- **THEN** the dashboard switches views instantly without full page reloads and maintains active timeframe filters

#### Scenario: Mobile responsive dashboard navigation
- **WHEN** a user opens the dashboard on a mobile viewport
- **THEN** the navigation collapses to a responsive top bar with hamburger menu opening a slide-over drawer, and multi-column visual charts scale to fit the viewport smoothly

### Requirement: 3-Second Rapid Transaction Capture Pad
The application SHALL provide a prominent quick-capture interface accessible on the main screen and via keyboard shortcut (`N` key).
1. It SHALL feature an organized, structured category dropdown selector with category icons and clean borders, replacing scattered tag clusters.
2. It SHALL default to the last-used or primary spend account.
3. It SHALL support rapid amount entry with inline arithmetic calculation.
4. Completing an entry SHALL immediately update the daily allowance and feed without full page reload.

#### Scenario: Organized category selection in quick capture
- **WHEN** a user opens the quick-capture modal and selects a category from the organized category dropdown
- **THEN** the category is cleanly selected with visual icon and name, without scattering unwieldy tag chips across the form

#### Scenario: Desktop keyboard shortcut capture
- **WHEN** a user presses `N` on any screen
- **THEN** the quick-capture modal/sheet opens immediately with the amount input auto-focused

### Requirement: Daily Spending Pulse Glance
The Pulse screen SHALL prominently display:
1. `Batas Belanja Hari Ini`: The dynamic daily spending allowance.
2. `Pengeluaran Hari Ini`: The total amount spent today.
3. `Laju Pengeluaran`: A visual indicator showing whether spending is on-track relative to cycle days elapsed.

#### Scenario: Over-budget visual feedback
- **WHEN** spending today exceeds the daily allowance
- **THEN** the pulse tile displays a clear, calm status indicator reflecting the pace adjustment for remaining cycle days without punitive scoring or guilt dialogs

### Requirement: Anti-AI-Slop Visual Design Standard
The design system SHALL adhere to modern digital banking craftsmanship standards (Bank Jago, Bibit, SeaBank):
1. **Typography:** Contemporary geometric Outfit font paired with tabular figures (`font-variant-numeric: tabular-nums`) for currency and timestamps.
2. **Seamless Surface Elevation:** Seamless floating card surfaces separated by subtle background tonal contrast and soft ambient shadows (`0 4px 20px -2px rgba(0,0,0,0.03)` light / `rgba(0,0,0,0.35)` dark) rather than harsh wireframe borders.
3. **Accents:** Emerald green for Income/Cash In, coral/rose for Expense/Cash Out, and cobalt blue for Transfers.
4. **Prohibitions:** The UI SHALL NOT contain harsh box outlines, rainbow gradients, blurry purple glow drop-shadows, 0–100 health meters, or unformatted text blocks.

#### Scenario: Dark mode contrast validation
- **WHEN** dark mode is active
- **THEN** text satisfies WCAG AA contrast against surface colors and cards float seamlessly over dark backgrounds with subtle hairline separation

### Requirement: Quick Capture Linking to Goals and Obligations
The global transaction quick-capture interface (`N`) SHALL allow optional direct linking to savings goals and debt obligations:
1. When recording an expense or transfer, the user can select an active Goal (marking it as a milestone contribution) or an active Obligation (marking it as a debt repayment).
2. Upon submission, the transaction SHALL record the corresponding `goal_id` or `obligation_id` and automatically increment the goal's `current_amount` or decrement the obligation's `remaining_amount` in the database.

#### Scenario: Contributing to a goal via quick capture
- **WHEN** the user presses `N`, enters `500000`, selects account "Main Bank", and chooses goal "Emergency Fund"
- **THEN** an expense of 500,000 IDR is recorded and linked to "Emergency Fund", and the goal's saved amount increases by 500,000 IDR immediately

