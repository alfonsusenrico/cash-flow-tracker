## MODIFIED Requirements

### Requirement: Interactive Design System Preset Switcher
The application SHALL provide an interactive design system switcher supporting 4 distinct modern styles with seamless, borderless digital bank aesthetics:
1. `swiss` (Swiss Digital Bank / Default): Seamless floating card surfaces on subtle canvas, hairline/borderless dividers, Outfit rounded geometric typography, high-contrast electric mint & coral financial signals, inspired by Bank Jago, Bibit, and SeaBank.
2. `nordic` (Nordic Obsidian / Raycast): Cool slate & titanium canvas, Plus Jakarta Sans typography, JetBrains Mono tabular figures, indigo primary brand accent.
3. `bauhaus` (Modern Bauhaus / Apple Card): Warm alabaster & midnight canvas, Outfit rounded geometric typography, soft pill button geometry, forest emerald & warm rose accents.
4. `stripe` (Executive FinTech / Stripe): Crisp ice-grey canvas, Inter Display typography, SF Mono tabular figures, royal cobalt blue primary brand accent.

#### Scenario: Switching design system presets in real-time
- **WHEN** a user selects any design preset from the TopBar or Settings switcher
- **THEN** the application immediately applies the chosen preset via `data-preset` attribute on `document.documentElement` without full page reload, updating fonts, colors, border radii, and button styles instantly

#### Scenario: Persisting design system preference
- **WHEN** a user selects a design system preset
- **THEN** the selection is saved to `localStorage` under `theme_preset` and restored on subsequent visits across all screens
