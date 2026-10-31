## MODIFIED Requirements

### Requirement: Modern Button Styling System
The application SHALL provide a consistent, modern button styling hierarchy.

#### Scenario: Required behavior and constraints
- **WHEN** the application renders an action button for the active design preset
- **THEN** the following required behavior and constraints hold:

  The application SHALL provide a consistent, modern button styling hierarchy:
  1. **Primary Action Buttons**: Solid background adhering to active preset colors, subtle 1px border, high-contrast text, smooth hover elevation, and tactile press scaling (`:active:scale-[0.98]`). Text and icons on the primary colour SHALL use `--primary-contrast` and SHALL measure at least 4.5:1 (WCAG 2.2 AA) against both `--primary` and `--primary-hover` in light and dark themes.
  2. **Secondary / Outlined Buttons**: Clean border (`var(--border)`), surface background (`var(--surface)`), and subtle hover state.
  3. **Preset-Adaptive Geometry**: Corner radii SHALL dynamically adapt to the active preset (`--radius-btn`), using compact geometry for Swiss Tech (`8px`), sleek curves for Nordic (`10px`), pill geometry for Bauhaus (`9999px`), and structured geometry for Stripe (`8px`).

#### Scenario: Tactile button press feedback
- **WHEN** a user clicks or taps any primary or secondary action button
- **THEN** the button provides instant tactile feedback with smooth micro-transition and active scale dampening

#### Scenario: Primary button text contrast
- **WHEN** a primary action button such as "Simpan profil" or the top-bar "Catat" is shown at rest or under the pointer, in the light or the dark theme
- **THEN** its label, icon and shortcut badge measure at least 4.5:1 against the button's background, and the coral background at rest is unchanged
