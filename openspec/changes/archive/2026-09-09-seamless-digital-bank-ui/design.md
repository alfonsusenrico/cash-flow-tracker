## Context

The user expressed a strong preference for the simplicity and contrast of Swiss Tech combined with the geometric typography of Modern Bauhaus (**Outfit**), but requested that card borders become **seamless** rather than prominently visible, citing leading Indonesian digital bank applications like **Bank Jago**, **Bibit**, and **SeaBank**.

## Goals / Non-Goals

**Goals:**
- Eliminate hard, wireframe borders on cards, bento blocks, ribbons, and lists across the entire dashboard.
- Introduce soft ambient elevation and subtle tonal background shifts (`var(--surface)` on `var(--bg)` with `box-shadow: 0 4px 20px -2px rgba(0, 0, 0, 0.03)` light / `rgba(0, 0, 0, 0.35)` dark) so cards feel seamlessly embedded.
- Adopt **Outfit** as the primary font for display, headings, and body across the default / Swiss Digital Bank theme, retaining tabular digits for financial alignment.
- Increase container corner radii to 20px (`--radius-card: 20px`) and buttons to 12px / pill curvature (`--radius-btn: 12px`), giving the tactile, ergonomic feel of modern digital banking.
- Retain subtle hairline borders (`rgba(0, 0, 0, 0.04)` light / `rgba(255, 255, 255, 0.05)` dark) to preserve visual boundary accessibility without clutter.

**Non-Goals:**
- Altering business logic, database queries, or backend endpoints.

## Decisions

### 1. Seamless Card Tokens in `tokens.css`
- **Decision:** Set `--color-rule: rgba(0, 0, 0, 0.04)` for light mode and `rgba(255, 255, 255, 0.05)` for dark mode on the default preset, with `--radius-card: 20px` and soft diffused ambient shadows.
- **Rationale:** Mimics Bank Jago and Bibit where containers do not announce themselves through heavy outlines, but float naturally on the background canvas.

### 2. Outfit Typography Pairing
- **Decision:** Map `--font-display: "Outfit", sans-serif` and `--font-body: "Outfit", sans-serif` in `:root, :root[data-preset="swiss"]`.
- **Rationale:** Directly satisfies the user's preference for Bauhaus typography while maintaining Swiss Tech's greyscale data focus.

## Risks / Trade-offs

- **[OLED Contrast]** → Ensure dark mode cards maintain distinct visibility against the background canvas via subtle lightness shift (`#13151B` cards against `#0A0B0E` canvas).
