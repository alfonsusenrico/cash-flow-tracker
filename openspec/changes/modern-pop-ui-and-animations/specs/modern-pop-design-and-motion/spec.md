## ADDED Requirements

### Requirement: Plus Jakarta Sans Typography Standard
The application SHALL use Plus Jakarta Sans as its primary display and body typeface across all views, with tabular figure support for numeric alignment.

#### Scenario: Verify font stack and tabular numerals
- **GIVEN** the application root layout is loaded
- **WHEN** text elements and currency amounts are rendered
- **THEN** the body font family SHALL resolve to `--font-jakarta`, Plus Jakarta Sans
- **AND** financial figures SHALL retain `font-variant-numeric: tabular-nums` to prevent horizontal jitter.

### Requirement: Pop Color Tokens and Translucent Candy Surfaces
The design system SHALL define high-contrast, energetic pop semantic colors and translucent tinted card surfaces in dark and light modes.

#### Scenario: Render pop tokens on financial surfaces
- **GIVEN** a user is viewing the Pulse or Insights dashboard
- **WHEN** positive cash flow or income cards are rendered
- **THEN** the surface SHALL utilize the Electric Mint accent (`#00D09C`) with translucent background washes
- **AND** negative cash flow or expense surfaces SHALL utilize the Juicy Neon Rose accent (`#FF3B69`).

### Requirement: Tactile Spring Physics on Interactive Elements
Interactive buttons, cards, and keypad tiles SHALL exhibit tactile spring-like compression physics upon user interaction.

#### Scenario: User taps a button or keypad key
- **GIVEN** a button, chip, or keypad tile is pressed by the user
- **WHEN** the element enters `:active` state
- **THEN** the element SHALL smoothly compress (`transform: scale(0.96)`) using a fast cubic-bezier curve
- **AND** return to its rest state within 150ms upon release.

### Requirement: Animated Number Ticker Rolling
Key dashboard headline figures (e.g. daily allowance, liquid balance, and projected cycle outflow) SHALL animate smoothly when mounting or updating.

#### Scenario: Number ticker rolls up on page load
- **GIVEN** the user opens the Pulse or Insights dashboard
- **WHEN** data loads with a target amount of Rp 150.000
- **THEN** the number counter SHALL smoothly increment from zero to the target amount over 350-400ms using an ease-out timing curve.

### Requirement: Quick Capture Micro-Celebration
The Quick Capture transaction modal SHALL provide immediate positive visual feedback upon successful submission.

#### Scenario: Transaction saved micro-burst
- **GIVEN** the user submits an expense in the Quick Capture modal
- **WHEN** the mutation succeeds
- **THEN** the submit button SHALL briefly transition to a green celebratory checkmark state
- **AND** the modal SHALL dismiss smoothly following the confirmation state.
