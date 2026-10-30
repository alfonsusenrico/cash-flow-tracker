## REMOVED Requirements

### Requirement: Daily Spending Pulse Glance
**Reason**: A dynamic daily allowance and pace indicator imply an unsupported future payday and spending pattern.
**Migration**: Show observed spending and configured budget and commitment information with clear period labels.

## MODIFIED Requirements

### Requirement: 3-Second Rapid Transaction Capture Pad
The application SHALL provide a prominent quick-capture interface accessible on the main screen and via keyboard shortcut (`N` key).

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens and completes rapid transaction capture
- **THEN** it SHALL provide an organized category selector with icons, default to the last-used or primary spend account, support inline arithmetic, and refresh observed figures and activity without a full page reload

#### Scenario: Organized category selection in quick capture
- **WHEN** a user opens the quick-capture modal and selects a category
- **THEN** the category is selected with its icon and name

#### Scenario: Desktop keyboard shortcut capture
- **WHEN** a user presses `N` on any screen
- **THEN** quick capture opens with the amount input auto-focused
