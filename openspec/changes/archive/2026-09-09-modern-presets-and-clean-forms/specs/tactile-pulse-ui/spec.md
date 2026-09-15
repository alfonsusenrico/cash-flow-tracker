## MODIFIED Requirements

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
