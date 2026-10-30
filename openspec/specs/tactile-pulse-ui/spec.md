# tactile-pulse-ui Specification

## Purpose
Define rapid daily transaction capture, spending awareness and shared financial navigation and interaction patterns.

## Requirements

### Requirement: 3-Screen Information Architecture
The web application SHALL organize user navigation into a dual-mode information architecture.

#### Scenario: Required behavior and constraints
- **WHEN** the application renders navigation for the current viewport
- **THEN** the following required behavior and constraints hold:

  The web application SHALL organize user navigation into a dual-mode information architecture:
  1. On desktop viewports ($\ge 1024\text{px}$), the system SHALL host navigation inside a persistent left-hand sidebar accessing 5 executive workspaces (`/`, `/ledger`, `/insights`, `/accounts`, `/goals`) with a multi-column command center layout.
  2. On mobile viewports ($< 1024\text{px}$), the system SHALL host navigation inside a dedicated bottom navigation bar (`BottomNav`) accessing the 4 core views (`Beranda`, `Transaksi`, `Analisis`, `Dompet`) plus a center quick-add trigger, replacing the slide-over hamburger drawer with native thumb-zone navigation.

#### Scenario: Navigating between executive dashboard screens
- **WHEN** a user selects any item from the persistent left sidebar navigation on desktop
- **THEN** the dashboard switches views instantly without full page reloads and maintains active timeframe filters

#### Scenario: Mobile responsive dashboard navigation
- **WHEN** a user opens the dashboard on a mobile viewport
- **THEN** the navigation displays a dedicated thumb-friendly bottom bar with active indicator badges
- **AND** the desktop sidebar and hamburger slide-over menu are hidden in favor of direct 1-tap tab switching

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

### Requirement: Anti-AI-Slop Visual Design Standard
The design system SHALL adhere to modern digital banking craftsmanship standards (Bank Jago, Bibit, SeaBank).

#### Scenario: Required behavior and constraints
- **WHEN** the application renders its financial interface
- **THEN** the following required behavior and constraints hold:

  The design system SHALL adhere to modern digital banking craftsmanship standards (Bank Jago, Bibit, SeaBank):
  1. **Typography:** Contemporary geometric Outfit font paired with tabular figures (`font-variant-numeric: tabular-nums`) for currency and timestamps.
  2. **Seamless Surface Elevation:** Seamless floating card surfaces separated by subtle background tonal contrast and soft ambient shadows (`0 4px 20px -2px rgba(0,0,0,0.03)` light / `rgba(0,0,0,0.35)` dark) rather than harsh wireframe borders.
  3. **Accents:** Emerald green for Income/Cash In, coral/rose for Expense/Cash Out, and cobalt blue for Transfers.
  4. **Prohibitions:** The UI SHALL NOT contain harsh box outlines, rainbow gradients, blurry purple glow drop-shadows, 0–100 health meters, or unformatted text blocks.

#### Scenario: Dark mode contrast validation
- **WHEN** dark mode is active
- **THEN** text satisfies WCAG AA contrast against surface colors and cards float seamlessly over dark backgrounds with subtle hairline separation

### Requirement: Quick Capture Linking to Goals and Obligations
The global transaction quick-capture interface (`N`) SHALL allow optional direct linking to savings goals and debt obligations.

#### Scenario: Required behavior and constraints
- **WHEN** an owner links a Quick Capture transaction to a goal or debt
- **THEN** the following required behavior and constraints hold:

  The global transaction quick-capture interface (`N`) SHALL allow optional direct linking to savings goals and debt obligations:
  1. When recording an expense or transfer, the user can select an active Goal (marking it as a milestone contribution) or an active Obligation (marking it as a debt repayment).
  2. Upon submission, the transaction SHALL record the corresponding `goal_id` or `obligation_id` and automatically increment the goal's `current_amount` or decrement the obligation's `remaining_amount` in the database.

#### Scenario: Contributing to a goal via quick capture
- **WHEN** the user presses `N`, enters `500000`, selects account "Main Bank", and chooses goal "Emergency Fund"
- **THEN** an expense of 500,000 IDR is recorded and linked to "Emergency Fund", and the goal's saved amount increases by 500,000 IDR immediately

### Requirement: Animated Headline Figures
Headline money figures on the Beranda balance card, the Beranda cash-flow matrix and the Rekening net-worth summary SHALL move from their previous value to a new value over about 380 ms with an ease-out curve. When the viewer requests reduced motion, the final value SHALL be shown at once.

#### Scenario: Balance changes
- **GIVEN** the Beranda balance card shows Rp 100.000
- **WHEN** the balance changes to Rp 150.000
- **THEN** the figure SHALL count from Rp 100.000 to Rp 150.000 within about 380 ms and end on the exact value

#### Scenario: Reduced motion
- **GIVEN** the operating system requests reduced motion
- **WHEN** a headline figure changes
- **THEN** the new value SHALL be shown immediately without intermediate values

### Requirement: Quick Capture Save Confirmation
After Quick Capture saves a transaction or movement, the submit button SHALL show a brief success state (`✓ Tersimpan!`, or `✓ Berhasil dipindahkan` for a movement) and the modal SHALL close about 450 ms later. If the record is saved but its receipt upload fails, the modal SHALL stay open with an error message instead.

#### Scenario: Transaction saved
- **WHEN** the owner submits an expense and the save succeeds
- **THEN** the submit button SHALL show `✓ Tersimpan!` and the modal SHALL close shortly afterwards

#### Scenario: Receipt upload fails after the save
- **WHEN** the transaction is saved but its receipt upload fails
- **THEN** the modal SHALL stay open and say that the transaction was saved but the receipt was not uploaded
