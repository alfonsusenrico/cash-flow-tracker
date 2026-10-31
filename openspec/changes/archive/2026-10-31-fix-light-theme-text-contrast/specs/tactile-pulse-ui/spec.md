## MODIFIED Requirements

### Requirement: Anti-AI-Slop Visual Design Standard
The design system SHALL adhere to modern digital banking craftsmanship standards (Bank Jago, Bibit, SeaBank).

#### Scenario: Required behavior and constraints
- **WHEN** the application renders its financial interface
- **THEN** the following required behavior and constraints hold:

  The design system SHALL adhere to modern digital banking craftsmanship standards (Bank Jago, Bibit, SeaBank):
  1. **Typography:** Plus Jakarta Sans for interface text and Geist Mono for numeric code text, both self-hosted from the app's own origin as defined by `web-font-hosting`, paired with tabular figures (`font-variant-numeric: tabular-nums`) for currency and timestamps.
  2. **Seamless Surface Elevation:** Seamless floating card surfaces separated by subtle background tonal contrast and soft ambient shadows (`0 4px 20px -2px rgba(0,0,0,0.03)` light / `rgba(0,0,0,0.35)` dark) rather than harsh wireframe borders.
  3. **Accents:** Emerald green for Income/Cash In, coral/rose for Expense/Cash Out, and cobalt blue for Transfers. Spring Lime and coral keep their vivid colours for fills (navigation pills, chart peaks, buttons, chart marks); lime and coral used as text or as a meaningful icon on a theme surface SHALL use a text ink of the same hue (darker than the fill in the light theme), measuring at least 4.5:1 for text and 3:1 for icons against the surface it sits on in the light and the dark theme.
  4. **Prohibitions:** The UI SHALL NOT contain harsh box outlines, rainbow gradients, blurry purple glow drop-shadows, 0–100 health meters, or unformatted text blocks.

#### Scenario: Dark mode contrast validation
- **WHEN** dark mode is active
- **THEN** text satisfies WCAG AA contrast against surface colors and cards float seamlessly over dark backgrounds with subtle hairline separation

#### Scenario: Light theme lime and coral text contrast
- **WHEN** the light theme is active and a page shows lime text such as "Kelola →" or "Lihat Semua →", or coral text such as an expense amount, the "Uang Keluar" badge or "+ Tambah Tagihan"
- **THEN** the text measures at least 4.5:1 against the surface it sits on, it still reads as lime or coral, and the lime pill, chart peak and coral fills are unchanged
