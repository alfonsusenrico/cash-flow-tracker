# Design - Financial Manager

A locked design system for the operational app. Every page redesign should read this file before emitting code. Do not regenerate per page; extend this file when the system needs to grow.

## Genre
modern-minimal, utilitarian, austere

## Macrostructure Family
- App pages: Ledger Workbench. Persistent navigation, compact command bar, dense content grid, and right-side detail drawers where needed.
- Review pages: Cycle Review. Top summary strip, primary chart/table body, supporting breakdowns below.
- Registry pages: Registry + Detail. Filter row, table/list body, modal or drawer details.

## Theme
- `--color-paper` oklch(98% 0.006 96)
- `--color-paper-2` oklch(95% 0.01 96)
- `--color-surface` oklch(100% 0 0)
- `--color-ink` oklch(20% 0.025 255)
- `--color-ink-2` oklch(42% 0.028 255)
- `--color-muted` oklch(57% 0.026 255)
- `--color-rule` oklch(88% 0.014 255)
- `--color-accent` oklch(57% 0.15 148)
- `--color-focus` oklch(62% 0.16 148)

## Typography
- Display: Inter, weight 700, normal
- Body: Inter, weight 400-600
- Mono: ui-monospace / SFMono-Regular
- Display tracking: 0
- Type scale anchor: compact dashboard scale, no viewport-width font sizing

## Spacing
4-point named scale. Pages must prefer named tokens and shared primitives over raw spacing.

## Motion
- Reveal pattern: none by default; data changes should not animate layout.
- UI motion: transform and opacity only, 160-220ms.
- Reduced-motion fallback: opacity-only, <= 150ms.

## Microinteractions Stance
- Silent success. Avoid decorative toasts for normal CRUD.
- Hover is quiet and table-focused.
- Focus rings are immediate and visible.

## CTA Voice
- Primary CTA: compact filled green, 8px radius, verb-first copy.
- Secondary CTA: neutral outline, same rhythm.
- Destructive CTA: red outline or fill only at confirmation points.

## Per-Page Allowances
- App pages must not use hero enrichment or marketing layout.
- Data-dense pages may use cards only for repeated items, modals, and framed tools.
- Tables and filters are first-class surfaces, not content inside decorative cards.

## What Pages Must Share
- Left navigation and top command bar.
- Inter typography and tabular financial numbers.
- Green only for positive/primary action; red only for loss/destructive/outflow.
- 8px or smaller component radius except avatar and toggles.
- Shared table, button, input, badge, progress, card, and drawer styling.

## What Pages May Differ On
- Density of the grid.
- Whether detail opens in a drawer or modal.
- Which summary metrics are surfaced first.

## Exports

### tokens.css
See `frontend/tokens.css`.
