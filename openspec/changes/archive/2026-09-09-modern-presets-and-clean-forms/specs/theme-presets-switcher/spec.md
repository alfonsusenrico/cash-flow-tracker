## ADDED Requirements

### Requirement: Interactive Design System Preset Switcher
The application SHALL provide an interactive design system switcher supporting 4 distinct modern minimalist styles:
1. `swiss` (Swiss Tech / Vercel): Monochrome greyscale canvas, razor-thin 1px borders, Geist Sans typography, Geist Mono tabular figures, high-contrast electric mint & coral financial signals.
2. `nordic` (Nordic Obsidian / Raycast): Cool slate & titanium canvas, Plus Jakarta Sans typography, JetBrains Mono tabular figures, indigo primary brand accent.
3. `bauhaus` (Modern Bauhaus / Apple Card): Warm alabaster & midnight canvas, Outfit rounded geometric typography, soft pill button geometry, forest emerald & warm rose accents.
4. `stripe` (Executive FinTech / Stripe): Crisp ice-grey canvas, Inter Display typography, SF Mono tabular figures, royal cobalt blue primary brand accent.

#### Scenario: Switching design system presets in real-time
- **WHEN** a user selects any design preset from the TopBar or Settings switcher
- **THEN** the application immediately applies the chosen preset via `data-preset` attribute on `document.documentElement` without full page reload, updating fonts, colors, border radii, and button styles instantly

#### Scenario: Persisting design system preference
- **WHEN** a user selects a design system preset
- **THEN** the selection is saved to `localStorage` under `theme_preset` and restored on subsequent visits across all screens

### Requirement: Modern Button Styling System
The application SHALL provide a consistent, modern button styling hierarchy:
1. **Primary Action Buttons**: Solid background adhering to active preset colors, subtle 1px border, high-contrast text, smooth hover elevation, and tactile press scaling (`:active:scale-[0.98]`).
2. **Secondary / Outlined Buttons**: Clean border (`var(--border)`), surface background (`var(--surface)`), and subtle hover state.
3. **Preset-Adaptive Geometry**: Corner radii SHALL dynamically adapt to the active preset (`--radius-btn`), using compact geometry for Swiss Tech (`8px`), sleek curves for Nordic (`10px`), pill geometry for Bauhaus (`9999px`), and structured geometry for Stripe (`8px`).

#### Scenario: Tactile button press feedback
- **WHEN** a user clicks or taps any primary or secondary action button
- **THEN** the button provides instant tactile feedback with smooth micro-transition and active scale dampening
