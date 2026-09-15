## Context

The Cash Flow Tracker UI uses Tailwind CSS, Next.js App Router, and CSS custom properties (`tokens.css`). While the previous Bibit-inspired emerald design established solid visual hierarchy, the user desires simpler, more modern aesthetic alternatives (Swiss Tech, Nordic Obsidian, Modern Bauhaus, Executive FinTech) and requested that these presets be directly previewable and toggleable in the live UI so they can evaluate and select their preference. Additionally, the Quick Capture modal currently renders categories as an unwieldy wrap of chip buttons ("shattered tags"), which should be replaced by a clean, organized dropdown selector alongside improved button interactions.

## Goals / Non-Goals

**Goals:**
- Provide a real-time style preset selector (TopBar dropdown / pill and Settings modal) toggling between 4 modern minimalism design systems (`swiss`, `nordic`, `bauhaus`, `stripe`).
- Implement dynamic styling via `[data-preset="..."]` CSS custom property blocks in `tokens.css` that adapt surface colors, borders, typography, and button corner radii across both light and dark modes.
- Load Google Fonts for Geist, Plus Jakarta Sans, Outfit, and JetBrains Mono alongside Inter, switching the root font family based on active preset.
- Re-architect `QuickCaptureModal.tsx` to replace scattered category chips with an organized category select dropdown with icons and clean borders.
- Polish button ergonomics with tactile feedback (`active:scale-[0.98]`), consistent padding, and preset-adaptive corner radii (`var(--radius-btn)`).

**Non-Goals:**
- Creating custom user color builders or full CSS theme customizers.
- Modifying backend schemas or API contracts (all changes are strictly visual presentation and form UI).

## Decisions

### 1. Attribute-Based Theme Switching (`data-preset` on `<html>`)
- **Decision:** Set `data-preset="swiss" | "nordic" | "bauhaus" | "stripe"` on `document.documentElement` alongside existing `.dark` class.
- **Rationale:** Enables instant, zero-re-render stylesheet switching via pure CSS variables. All Tailwind classes referencing CSS variables (`var(--bg)`, `var(--surface)`, `var(--border)`, `var(--primary)`, `var(--font-body)`) adapt instantaneously without lag or DOM recreation.
- **Alternatives Considered:** Component-level inline style overrides (too fragile, causes flash of unstyled content).

### 2. Multi-Preset Typography Stack
- **Decision:** Import Geist, Plus Jakarta Sans, Outfit, and JetBrains Mono in `globals.css` and map `--font-body`, `--font-display`, and `--font-mono` inside each `[data-preset]` block.
- **Rationale:** Typography is 70% of modern minimalism. Each aesthetic has a signature typographic identity (Geist for Swiss, Plus Jakarta Sans for Nordic, Outfit for Bauhaus, Inter for Stripe).

### 3. Organized Dropdown for Transaction Categories
- **Decision:** Replace the `flex flex-wrap gap-1.5` chip list in `QuickCaptureModal.tsx` with a styled `<select>` (or custom icon dropdown) matching the existing account selector.
- **Rationale:** Conserves vertical modal height, eliminates visual clutter ("shattered tags"), and ensures scalable browsing regardless of how many categories exist.

## Risks / Trade-offs

- **[Font Loading Latency]** → Preload fonts in `globals.css` with `display=swap` so fallback system fonts render immediately without blocking.
- **[Dark Mode Contrast across Presets]** → Test each of the 4 presets in both light and dark modes to guarantee WCAG AA contrast for text, icons, and borders.
