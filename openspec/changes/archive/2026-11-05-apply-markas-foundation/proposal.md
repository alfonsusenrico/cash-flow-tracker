## Why

On 2026-11-05 the owner compared six design options and chose a combination:
- "I want 3, with combinated stylings (especially fonts) from 4 & 5, but the layout totally 3"
- "for any borders that colored and thick like border on add new transaction make it simple and clean"
- on the resulting option 6 "Markas+": "ok accepted"

The owner expects the app to match that prototype exactly. Every later page depends on the same fonts, colours, type scale, radii, borders, focus and controls. This change lays that foundation first, so the page changes only rearrange layout.

The phased plan (twelve changes, this is phase 1) is in the prototypes repository at `research/2026-11-05-markas-implementation-plan.md`. The prototype and its measurement reference are `design-options/option-6/` (`SPEC.md`, `styles.css`).

## Expected Outcome

- Every page reads in Inter, with amounts in Inter's tabular figures. Page titles, dialog titles and the brand are in Manrope.
- Only seven text sizes (12, 14, 16, 20, 24, 32, 44 px) and three weights (400, 500, 600) appear anywhere.
- Light pages sit on warm paper tones (`#F8F7F4`, `#F1EFEB`, `#E6E3DD`) and dark pages on soft charcoal (`#151517`, `#1C1D20`, `#26272B`), under the same coral accent.
- Every border is 1 px and neutral.
  - Clicking into a field, for example in Catat, shows a thin dark edge with a soft grey halo instead of a thick coral ring.
  - Keyboard focus on buttons and links is a 2 px neutral outline.
- Buttons, fields, segments, tabs, badges, chips, switches, menus, dialogs, sheets and toasts have the prototype's sizes, radii (6, 12 or 16 px) and states.
- Cards no longer carry shadows; only floating layers do.
- Numbers no longer count up when they change.
- Page layouts, routes, figures and behaviour stay as they are. The shell and the pages change in later phases.

## Approval

Approved on 2026-11-05. The owner accepted the design that day ("ok accepted") and then approved the phased plan and this change with "defaults ok, implement". All seven plan decisions take their recommended defaults:
1. Kakeibo and account-use colours: the prototype's values, used as dots and bars only (D4 option A).
2. Rekening debts: credit accounts only under Utang, plus a "Tagihan" link (phase 8).
3. Merging: each phase merges once the owner accepts it.
4. Test session: the one-line test-session notice stays.
5. Login and onboarding: foundation styling only.
6. The held branch `fix/selection-bars-and-tinjau` (64909ee): folded into the phases and not merged; its straight-mark style and guard test come into this phase.
7. Verification seed: a prototype-data verification seed and a development-only `/komponen` sheet, never in a production build (D12 option A).

## What Changes

- **Fonts:**
  - self-host Inter (variable, Latin) and Manrope (variable, Latin) from the app's origin with their licences;
  - remove Plus Jakarta Sans and Geist Mono; the pairing code and the API key prefix use Inter tabular figures, as in the prototype.
- **Figures:** prose uses lining figures with Inter's single-storey a (`cv11`). Every amount, count, time, date column, table, field, badge and chart label uses tabular figures.
- **Colour tokens** take the option-6 values in light and dark, mapped onto the app's semantic token names. New tokens are added for the dialog surface, field focus, the soft ring and the pressed accent.
  - Soft and tinted semantic fills become the neutral badge fill.
  - Kakeibo, use and flow marks take the prototype's mark colours.
- **Type scale:** the seven sizes and three weights become the only ones in the app. Every stray size (12.5, 13, 13.5, 15.5 px, `clamp()`) and weight (550 to 900) is snapped to the scale, and a test forbids them.
- **Geometry:** radii collapse to 6/12/16 under the prototype's names. Spacing and motion tokens are added. Card and control shadows are removed. Floating layers use the warm layer shadow.
- **Borders and focus:**
  - no border is thicker than 1 px or coloured, and no outline or ring is coloured, except the selection bars, group rails and focus;
  - error fields keep a neutral edge with the red message below;
  - controls show a 2 px neutral keyboard outline;
  - fields show a 1 px `--field-focus` edge and a 3 px soft ring.
- **Shared primitives** are restyled to option-6 `SPEC.md` §7: buttons (primary coral, secondary, quiet, destructive, link, icon), fields, money input, select, textarea, search, segmented control, tabs, badge, chip, quick chip, key cap, switch, checkbox, menu, dialog, sheet, toast, skeleton, empty and error states, the explanation popover and the scrim.
  - The dark "structural" primary and the green, sky and lavender solid buttons become the coral primary or the secondary button.
- **Selection marks:** add the shared straight selection bar and group rail, with a guard test against curved left marks. This is taken over from the held branch `fix/selection-bars-and-tinjau`. The existing curved marks (goal account picker, Grafik ring legend, Tinjau rows) move to it; their pages are otherwise unchanged.
- **Motion:** state changes take 140 ms and layers 200 ms with the prototype's curve, all off under reduced motion. **BREAKING (spec):** headline figures no longer animate.
- **Verification tooling:** versioned capture, measure and compare scripts under `scripts/verify/markas/`, and a development-only `/komponen` sheet.

## Non-goals

- No change to the shell (sidebar, top bar, title block, phone tab bar) or to any page's layout, grids, column order, breakpoints or content. Those are phases 2 to 11.
- No removal of page components that the audit marks redundant (KPI strips, explain underlines, hint strips, icon tiles, banners). Those are removed by their page's phase.
- No copy change, such as "Belanja harian" becoming "Pengeluaran harian"; that also happens in the page phases.
- Chart-specific tokens without a prototype counterpart (calendar heat levels, Peta Aliran band opacity, investment-kind steps) keep their values until their page phase.
- No backend, API, schema, data or companion change. Every money figure stays the same.
- Local verification only. Release and production stay with the owner through the approved pipeline after acceptance.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `design-tokens`:
  - the option-6 palette replaces the pre-redesign palette;
  - type families and scale, geometry and borders, neutral focus and field states, state and layer motion, and straight selection marks become requirements.
- `web-font-hosting`: the self-hosted families become Inter and Manrope.
- `tactile-pulse-ui`:
  - the pre-redesign "Anti-AI-Slop Visual Design Standard" is removed;
  - its rules move to `design-tokens` (type, surfaces, borders, elevation) and to three focused requirements: "Semantic Colour Meaning", "Kakeibo Pillar Marks" and "No Decorative Effects";
  - "Animated Headline Figures" is removed.
- `theme-presets-switcher`: the button hierarchy becomes option 6's (coral primary, secondary, quiet, destructive, link, icon) with its geometry and neutral focus.

Standards: UI and accessibility (WCAG 2.2 AA: contrast, focus visible, keyboard, reflow, reduced motion) and the copy register. No capability in `AGENTS.md` changes.

## Impact

- **Frontend:**
  - `frontend/tokens.css`, `frontend/tailwind.config.js`, `frontend/src/app/globals.css`, `frontend/src/app/layout.tsx`, `frontend/src/app/fonts/`;
  - the shared components in `components/ui` (Button, Input, FormField, Modal and the dialog bodies' controls) and `components/workspace` (primitives, overlays, explain, shortcuts);
  - class-level size, weight, radius, border and tint edits across `frontend/src` (about 50 files, mechanical);
  - `hooks/useAnimatedCounter.ts` (removed).
- **Tests:** `tokens.test.ts`, `fonts.test.ts`, the new `typeScale.test.ts`, `geometry.test.ts` and `selectionMarks.test.ts`, and component tests whose class or style assertions change.
- **Tooling:** `scripts/verify/markas/` (Node scripts driving a local headless browser over the Chrome DevTools Protocol; no new npm dependency) and a `/komponen` route that exists only when the build sets the local flag. `next.config.mjs` and `proxy.ts` gate it.
- **No change** to `nginx/default.conf`; the Content-Security-Policy stays `font-src 'self'`.
- No API, data or dependency change. Rollback is a revert of the merge commit.
