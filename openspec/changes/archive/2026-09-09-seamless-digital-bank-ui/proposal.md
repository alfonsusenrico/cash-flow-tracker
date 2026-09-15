## Why

While the Swiss Tech foundation provides data clarity, visible structural borders create a rigid wireframe feel. Modern digital banking applications (Bank Jago, Bibit, SeaBank) achieve a fluid, premium aesthetic through seamless surface separation (subtle background tonal shifts and diffused ambient elevation rather than hard border lines) paired with friendly, contemporary geometric typography (Outfit from Modern Bauhaus).

## What Changes

- **Seamless Borderless Surface Elevation**: Replace harsh 1px card borders with subtle tonal contrast and soft ambient shadows (`0 4px 20px -2px rgba(0,0,0,0.03)` light / `rgba(0,0,0,0.35)` dark), rendering cards, bento blocks, and ribbons as seamless, floating sheets.
- **Outfit Geometric Typography**: Set **Outfit** as the primary display and body typeface for the Swiss Tech / Default theme, delivering the friendly, modern warmth of Bank Jago and Bibit while maintaining tabular figure alignment for monetary numbers.
- **Ergonomic Curvatures**: Increase card corner radii from 12px to 20px (`--radius-card: 20px`) and button radii to 12px (`--radius-btn: 12px`), matching modern mobile banking UI standards.
- **Subtle Hairline Dividers**: Retain borders only as ultra-faint hairlines (`rgba(0,0,0,0.04)` light / `rgba(255,255,255,0.05)` dark) for high accessibility and dark mode depth without visual clutter.

## Capabilities

### Modified Capabilities
- `theme-presets-switcher`: Update the default and Swiss Tech preset to feature Outfit typography, seamless borderless elevation, and digital banking tonal tokens.
- `tactile-pulse-ui`: Update dashboard cards, bento boxes, and action pills to use seamless borderless elevation and rounded-2xl geometry.

## Impact

- **Frontend**:
  - `frontend/tokens.css`: Update Swiss Tech / default CSS custom properties (`--color-rule`, `--radius-card`, `--radius-btn`, `--font-display`, `--font-body`, `--shadow-sm`, `--shadow-md`).
  - `frontend/src/components/ui/StyleSwitcher.tsx`: Update Swiss Tech preset description to reflect "Seamless Digital Bank (Outfit Sans)".
  - `frontend/src/app/page.tsx`, `frontend/src/components/dashboard/*`: Enhance card surfaces to seamlessly blend into canvas with soft diffuse depth.
- **Backend**: None.
