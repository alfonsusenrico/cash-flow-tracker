## MODIFIED Requirements

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

## ADDED Requirements

### Requirement: Quick Capture Linking to Goals and Obligations
The global transaction quick-capture interface (`N`) SHALL allow optional direct linking to savings goals and debt obligations:
1. When recording an expense or transfer, the user can select an active Goal (marking it as a milestone contribution) or an active Obligation (marking it as a debt repayment).
2. Upon submission, the transaction SHALL record the corresponding `goal_id` or `obligation_id` and automatically increment the goal's `current_amount` or decrement the obligation's `remaining_amount` in the database.

#### Scenario: Contributing to a goal via quick capture
- **WHEN** the user presses `N`, enters `500000`, selects account "Main Bank", and chooses goal "Emergency Fund"
- **THEN** an expense of 500,000 IDR is recorded and linked to "Emergency Fund", and the goal's saved amount increases by 500,000 IDR immediately
