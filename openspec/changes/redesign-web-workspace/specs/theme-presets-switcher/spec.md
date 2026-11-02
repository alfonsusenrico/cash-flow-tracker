# Spec Delta

## REMOVED Requirements

### Requirement: Interactive Design System Preset Switcher
**Reason**: The four presets (`swiss`, `nordic`, `bauhaus`, `stripe`) are no longer offered; the app already clears `data-preset`, `theme_preset` and `theme_palette` on load. The approved workspace design has one design with a light and a dark theme.
**Migration**: The "Light, Dark and System Theme" requirement replaces it. Stored `theme_preset` and `theme_palette` values keep being removed on load, and a stored `theme` of `light` or `dark` is honoured.

## MODIFIED Requirements

### Requirement: Modern Button Styling System
The application SHALL provide a consistent button hierarchy built from the workspace tokens.

#### Scenario: Required behavior and constraints
- **WHEN** the application renders an action button
- **THEN** the following required behavior and constraints hold:

  The application SHALL provide a consistent button hierarchy:
  1. **Primary structural action**: the dark ink fill with its contrast ink, such as "+ Catat" in the top bar and the Catat button of the phone tab bar. Its label, icon and shortcut hint SHALL measure at least 4.5:1 (WCAG 2.2 AA) against the fill at rest and under the pointer, in light and dark themes.
  2. **Positive action**: the lime fill with the lime ink, such as "Simpan", measuring at least 4.5:1 at rest and under the pointer in both themes.
  3. **Secondary button**: the surface fill with the strong line token as a 1px border and a subtle hover fill.
  4. **Semantic action pills**: small actions in a semantic soft fill with its ink, such as "Jawab" (coral), "Pasangkan" (sky), "Pilih" (amber) and "Lihat" (neutral), each at least 4.5:1.
  5. **Geometry**: one fixed set of radii from the tokens (buttons about 11–12 px, cards about 20 px, pills fully rounded); there are no preset-dependent radii.
  6. **Destructive and debt action buttons**: solid rose fill from `--danger-solid` (`--danger-solid-hover` on hover) with `--danger-contrast` text, measuring at least 4.5:1 (WCAG 2.2 AA) in light and dark themes. A confirmation button SHALL keep this styling regardless of the styling of the control that opened it.
  7. Every button SHALL have a target of at least 24 by 24 CSS pixels and a visible focus ring from the focus token.

#### Scenario: Tactile button press feedback
- **WHEN** a user clicks or taps any primary or secondary action button
- **THEN** the button gives immediate visual feedback with a short transition, and with reduced motion requested no scaling animation is used

#### Scenario: Primary button text contrast
- **WHEN** a primary action button such as "+ Catat" or a positive action such as "Simpan profil" is shown at rest or under the pointer, in the light or the dark theme
- **THEN** its label, icon and shortcut badge measure at least 4.5:1 against the button's background

#### Scenario: Destructive button text contrast
- **WHEN** a destructive or debt action such as "Ya, lanjutkan", "Ya, hapus", "Cabut perangkat" or "+ Tagihan Baru" is shown at rest or under the pointer, in the light or the dark theme
- **THEN** its label measures at least 4.5:1 against the button's rose background

## ADDED Requirements

### Requirement: Light, Dark and System Theme
The web app SHALL offer "Terang", "Gelap" and "Ikuti sistem" themes, default to "Ikuti sistem", persist the choice in the browser, and apply it before the first paint so no page flashes the other theme. With "Ikuti sistem" the theme SHALL follow operating-system changes while the page is open.

#### Scenario: First visit
- **WHEN** a visitor with a dark operating-system theme opens any page for the first time
- **THEN** the page SHALL render dark from the first paint

#### Scenario: Persisted choice
- **WHEN** the owner chooses "Terang" and reloads `/ledger`
- **THEN** the page SHALL render light from the first paint, also when the operating system is dark

#### Scenario: Earlier stored value
- **WHEN** the browser still holds `theme = dark` from before the redesign
- **THEN** the app SHALL treat it as the "Gelap" choice

#### Scenario: Storage unavailable
- **WHEN** browser storage throws or is empty
- **THEN** the app SHALL follow the operating system and keep working without an error
