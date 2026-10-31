## MODIFIED Requirements

### Requirement: Anti-AI-Slop Visual Design Standard
The design system SHALL adhere to modern digital banking craftsmanship standards (Bank Jago, Bibit, SeaBank).

#### Scenario: Required behavior and constraints
- **WHEN** the application renders its financial interface
- **THEN** the following required behavior and constraints hold:

  The design system SHALL adhere to modern digital banking craftsmanship standards (Bank Jago, Bibit, SeaBank):
  1. **Typography:** Plus Jakarta Sans for interface text and Geist Mono for numeric code text, both self-hosted from the app's own origin as defined by `web-font-hosting`, paired with tabular figures (`font-variant-numeric: tabular-nums`) for currency and timestamps.
  2. **Seamless Surface Elevation:** Card surfaces SHALL be separated from the page by the tonal contrast between `--surface` and `--bg` and a 1px hairline `--border` (ink at 9% opacity in the light theme, white at 8% in the dark theme), never by heavy wireframe outlines. A raised surface SHALL use the `--shadow-xs`, `--shadow-sm` or `--shadow-md` token of `frontend/tokens.css`, where `--shadow-xs` is the 1px raise of plain cards, chips and buttons and stays lighter than `--shadow-sm` in both themes; deeper shadows are reserved for floating layers such as dialogs, menus, the phone navigation bar and the selection bar.
  3. **Accents:** Emerald green for Income/Cash In, coral/rose for Expense/Cash Out, sky blue for internal transfers ("Pindah Saldo") and lavender for investment top-ups. Spring Lime and coral keep their vivid colours for fills (navigation pills, chart peaks, buttons, chart marks); lime and coral used as text or as a meaningful icon on a theme surface SHALL use a text ink of the same hue (darker than the fill in the light theme), measuring at least 4.5:1 for text and 3:1 for icons against the surface it sits on in the light and the dark theme.
  4. **Prohibitions:** The UI SHALL NOT contain harsh box outlines, rainbow gradients, blurry purple glow drop-shadows, 0–100 health meters, or unformatted text blocks.

#### Scenario: Dark mode contrast validation
- **WHEN** dark mode is active
- **THEN** text satisfies WCAG AA contrast against surface colors and cards float seamlessly over dark backgrounds with subtle hairline separation

#### Scenario: Light theme lime and coral text contrast
- **WHEN** the light theme is active and a page shows lime text such as "Kelola →" or "Lihat Semua →", or coral text such as an expense amount, the "Uang Keluar" badge or "+ Tambah Tagihan"
- **THEN** the text measures at least 4.5:1 against the surface it sits on, it still reads as lime or coral, and the lime pill, chart peak and coral fills are unchanged

#### Scenario: Plain cards carry a subtle raise
- **WHEN** a card, chip or button styled with the `shadow-xs` utility renders in the light or the dark theme
- **THEN** its computed `box-shadow` SHALL be the `--shadow-xs` value of that theme (a 1px offset with at most 2px blur, lighter than `--shadow-sm`), and a surface that already has `--shadow-sm` or a deeper shadow SHALL keep it
