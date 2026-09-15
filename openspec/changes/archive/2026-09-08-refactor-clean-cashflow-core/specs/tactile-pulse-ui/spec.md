## ADDED Requirements

### Requirement: 3-Screen Information Architecture
The web application SHALL organize all user navigation into three primary views:
1. `/` (Pulse): Today's glanceable allowance, rapid quick-capture bar, and today's activity feed.
2. `/insights`: Category spending progress bars against monthly limits, daily spending histogram, and cycle comparison.
3. `/accounts`: Liquid account cards with instant account-to-account transfer modal.

The UI SHALL NOT display links or pages for Buckets, Allocations, Strategy Rules, Financial Goals, Debt Obligations, or Asset Holdings.

#### Scenario: Mobile navigation shows all primary screens
- **WHEN** a user opens the app on a mobile device
- **THEN** the bottom navigation exposes direct tabs for Pulse, Insights, and Accounts without clipping or hidden routes

#### Scenario: Desktop navigation layout
- **WHEN** a user opens the app on a desktop display
- **THEN** the screen displays an ergonomic header or sidebar linking to the 3 primary screens and account settings

### Requirement: 3-Second Rapid Transaction Capture Pad
The application SHALL provide a prominent quick-capture interface accessible on the main screen and via keyboard shortcut (`N` key).
1. It SHALL feature 1-tap category chips for recently used categories.
2. It SHALL default to the last-used or primary spend account.
3. It SHALL support rapid amount entry with inline arithmetic calculation.
4. Completing an entry SHALL immediately update the daily allowance and feed without full page reload.

#### Scenario: 1-Tap expense logging
- **WHEN** a user enters `35000` and taps the `Food` category chip
- **THEN** an expense of 35,000 IDR is recorded under the active account and Food category within 1 action, and the today view updates optimistically

#### Scenario: Desktop keyboard shortcut capture
- **WHEN** a user presses `N` on any screen
- **THEN** the quick-capture modal/sheet opens immediately with the amount input auto-focused

### Requirement: Daily Spending Pulse Glance
The Pulse screen SHALL prominent display:
1. `Safe to spend today`: The dynamic daily spending allowance.
2. `Spent today`: The total amount spent today.
3. `Cycle Pace`: A visual indicator showing whether spending is on-track relative to cycle days elapsed.

#### Scenario: Over-budget visual feedback
- **WHEN** spending today exceeds the daily allowance
- **THEN** the pulse tile displays a clear, calm status indicator reflecting the pace adjustment for remaining cycle days without punitive scoring or guilt dialogs

### Requirement: Anti-AI-Slop Visual Design Standard
The design system SHALL adhere to high craftsmanship standards:
1. **Typography:** Crisp Inter font with tabular figures (`font-variant-numeric: tabular-nums`) for currency and dates.
2. **Palette:** High-contrast neutral background (`#FAFAF9` light / `#0F1012` dark) with 1px structural borders.
3. **Accents:** Emerald green for Income/Cash In, coral/rose for Expense/Cash Out, and cobalt blue for Transfers.
4. **Prohibitions:** The UI SHALL NOT contain rainbow gradients, blurry purple glow drop-shadows, 0–100 health meters, or 24px-padded empty bubbly cards.

#### Scenario: Dark mode contrast validation
- **WHEN** dark mode is active
- **THEN** text satisfies WCAG AA contrast against `#0F1012` surface colors and borders remain subtle structural dividers
