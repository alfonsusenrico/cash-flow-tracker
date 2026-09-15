## MODIFIED Requirements

### Requirement: Anti-AI-Slop Visual Design Standard
The design system SHALL adhere to modern digital banking craftsmanship standards (Bank Jago, Bibit, SeaBank):
1. **Typography:** Contemporary geometric Outfit font paired with tabular figures (`font-variant-numeric: tabular-nums`) for currency and timestamps.
2. **Seamless Surface Elevation:** Seamless floating card surfaces separated by subtle background tonal contrast and soft ambient shadows (`0 4px 20px -2px rgba(0,0,0,0.03)` light / `rgba(0,0,0,0.35)` dark) rather than harsh wireframe borders.
3. **Accents:** Emerald green for Income/Cash In, coral/rose for Expense/Cash Out, and cobalt blue for Transfers.
4. **Prohibitions:** The UI SHALL NOT contain harsh box outlines, rainbow gradients, blurry purple glow drop-shadows, 0–100 health meters, or unformatted text blocks.

#### Scenario: Dark mode contrast validation
- **WHEN** dark mode is active
- **THEN** text satisfies WCAG AA contrast against surface colors and cards float seamlessly over dark backgrounds with subtle hairline separation
