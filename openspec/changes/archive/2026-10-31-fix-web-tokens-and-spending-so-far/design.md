# Design

## Context

See proposal.md for the problems. Facts that shape the approach:

- `AppLayout` sets `px-3 sm:px-6 lg:px-8` on `<main>` since c3b36c8. `executive-sidebar-layout` states `px-4 sm:px-6 lg:px-8`.
- Tailwind maps `rose-500/600/700` to `--color-expense` and `--color-expense-hover`. In dark mode `--color-expense-hover` is the light coral `#FF6675`, so `bg-rose-600 text-white` is 2.83:1 there. `confirmDangerClass` already worked around this with standard `rose-800/900` in dark (8.02:1 and 9.57:1).
- `ConfirmActionButton` passed its `className` to both the trigger and the confirm button. Every caller styles the trigger as a quiet icon button (`text-[var(--muted)]`, `hover:bg-rose-500/10`), and `tailwind-merge` let those classes override the confirm button's white text.
- The undefined properties come from two eras. `--primary-light`, `--danger-light`, `--warning`, `--warning-light`, `--info`, `--info-light` and the `--color-accent` trio were defined in 1ae5c4a and dropped by a880522. `--accent-lime` was defined until a9b8867 and is still used with a `#66CC55` fallback. `--foreground` is a scaffold name from bb2725d.
- `Badge`, `MetricTile`, `ProgressBar` and `Card` (the only users of the status tokens and `.card-green`) are imported nowhere.
- `period_comparison` shortens the current window to the previous period's length (approved rule). On 31 October after a 30-day September, the current window ends at 30 October 23:59:59, so today's spending is outside it. The card printed that shortened `current.outflow` as "Uang keluar sejauh ini". `/dashboard/overview` and the comparison use the same `get_timeframe_window` bounds and the same reportable-expense and offset rules, so the full-window figure equals the overview's `total_outflow`.

## Decisions

1. **Gutter `px-4` on phones.** This implements the existing spec value. Larger breakpoints are unchanged.
2. **Destructive fill tokens.** `--danger-solid` `#E02038` (light) and `#9F1239` (dark), `--danger-solid-hover` `#9F1239` (light) and `#881337` (dark), and `--danger-contrast` `#FFFFFF`. These are the colours `confirmDangerClass` already rendered, so settings confirm buttons look the same. White text measures 4.74, 8.02, 8.02 and 9.57:1. `--danger`, `--color-expense` and the rose Tailwind mapping are unchanged, because they also colour expense amounts and charts. Rejected: dark text on the coral (destructive buttons would look like primary buttons), and remapping `rose-600/700` in dark mode (it would also darken every `text-rose-600` expense figure on the dark background).
3. **The confirm button keeps its own classes.** `ConfirmActionButton` applies `className` to the trigger only. No caller styles the confirm button on purpose.
4. **Define, or point at existing tokens.**
   - `--foreground` → `--text` at its 11 uses on the ledger page. Rendering is unchanged because the elements inherited `--text`.
   - `--accent-lime: #66CC55` in `:root` (AGENTS.md "Signature Spring Lime"); the `,#66CC55` fallbacks are removed as redundant.
   - `--primary-light` → `--primary-soft`, `--danger-light` → `--danger-soft`, `--warning-light` → `--color-warning-soft`.
   - `--warning` `#B45309` / `#FBBF24` and `--info` `#0369A1` / `#38BDF8`, with `--info-light` `#E0F2FE` / `rgba(56, 189, 248, 0.16)`. These are the amber and sky pastel-badge pairs from the live palette, and the ink reaches 4.51 to 11.31:1 on its pastel and on surfaces. The old `--color-warning` `#F59E0B` measures about 2:1 as text, so it was not reused.
   - `.card-green` trio: `#047857` → `#065F46` with white ink (5.48 and 7.68:1) in both themes. The 1ae5c4a green with white ink measured about 3.6:1.
5. **`current_to_date` in the comparison response.** The service reports the selected period from its start to now with the same reportable-flow rules. It reuses the compared figures when the window was not shortened, so a second query runs only when it was. The card uses it for all three "sejauh ini" sentences. The comparison figures and rule are unchanged. Rejected: dropping the shortening when the previous period has no data (it would not fix the "too short to compare" sentence and would change the Analisis window), and computing the figure in the card (the card has no access to the window or the reportable-flow rules).
6. **Tests.** `tokens.test.ts` scans every non-test `.ts`/`.tsx` file under `src` plus both stylesheets, with an empty allowlist. It checks text/background pairs (primary, danger, warning, info, accent) in both themes, compositing translucent tokens over `--surface`. A `ConfirmActionButton` test covers the class leak. Backend tests cover the 31 October case and agreement with `/dashboard/overview`. A card test covers the "sejauh ini" sentences.

## Risks / Trade-offs

- [The phone content area is 8 px narrower] → All seven pages re-checked at 390 and 320 px in both themes for overflow and off-screen elements.
- [Light-theme hover on destructive buttons jumps from `#E02038` to `#9F1239`] → This is the existing `confirmDangerClass` behaviour, now shared.
- [Defining tokens for unused components changes no rendered page] → They are covered by the contrast test so a future use starts accessible.

## Migration Plan

The response field is additive, so old clients ignore it. Frontend and backend ship together through the normal release. Rollback is reverting the commit.

## Open decisions for owner

- **Lime as text on light surfaces.** "Kelola →", "Lihat Semua →" and the `+` quick-action icons on phone Beranda use `#66CC55` on the light paper at 1.71:1 (measured; 2.03:1 on white), where AA needs 4.5:1 for text and 3:1 for icons. Dark theme measures 9.97:1. A darker light-theme lime for text would fix it; it was not in this scope.
- **Coral expense text and unused coral badges.** Coral text on white is 3.60:1 (pre-existing across expense amounts). `Badge` green and red variants would be 3.09:1 if used.
- **Unused components.** `Badge`, `MetricTile`, `ProgressBar`, `Card` and the `.workbench-page` rule are referenced by no page.
