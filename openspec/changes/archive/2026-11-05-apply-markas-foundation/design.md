## Context

The owner accepted option 6 "Markas+" on 2026-11-05. The measurement reference is
`cash-flow-tracker-prototypes/design-options/option-6/SPEC.md`. `styles.css` mirrors it and wins where the two differ:
- the light layer shadow is `0 16px 48px rgba(40,34,26,.14), 0 2px 6px rgba(40,34,26,.06)`, not SPEC §5's stale value;
- the first content sits at `y = 192`, not SPEC §12's 176.

Today the app styles itself in three places:
- `frontend/tokens.css`: 259 lines, the pre-redesign "Coral Terminal" values on semantic names;
- `frontend/src/app/globals.css`: 1,050 lines, 245 `.ws-*` classes with half-pixel sizes and weights up to 900;
- Tailwind utilities and CSS modules in about 50 components: 264 `rounded-*`, 251 heavy weight classes, 108 arbitrary pixel sizes and 109 tinted semantic fills.

Fonts are Plus Jakarta Sans and Geist Mono, self-hosted through `next/font/local` (`web-font-hosting`). The dark theme is the `.dark` class set before first paint by `THEME_SCRIPT`.

The audit (`design-options/audit/AUDIT.md`) found the fatigue comes from type, emphasis and layout rules, not colour. This phase fixes the rules every page shares; the shell and pages follow in phases 2 to 11 of the plan.

## Goals / Non-Goals

**Goals:**
- Only Inter and Manrope, seven sizes and three weights render anywhere.
- Every colour, radius, border, shadow, focus and motion value comes from option 6.
- The shared controls look and behave exactly like option 6's.
- A measurable gate against the prototype's component sheet exists.

**Non-Goals:**
- No change to page structure, grids, breakpoints, copy or content.
- No backend, API or data change.
- No new npm dependency.

## Decisions

### D1. Fonts: the prototype's own Google Fonts files, committed and served by `next/font/local`

- Download the Latin-subset variable `woff2` files that `fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Manrope:wght@600&display=swap` serves to a current browser. These are the exact files the owner saw in the prototype.
- Commit them to `frontend/src/app/fonts/` with the OFL texts from `google/fonts` (`ofl/inter/OFL.txt`, `ofl/manrope/OFL.txt`), and record each file's SHA-256 and `fvar` axes in `verification.md`.
- Declare Inter with `weight: "400 600"` and Manrope with `weight: "600"`. A heavier request then clamps to 600 instead of rendering 700 to 900. Add `font-synthesis-weight: none`.
- CSS variables `--font-inter` and `--font-manrope` feed `--font-sans` and `--font-display`.

Rejected alternatives:
- Google Fonts at runtime or `next/font/google`: these break `font-src 'self'` and the build-without-network rule of `web-font-hosting`.
- The upstream Inter release from rsms: it is a different build from the one the prototype rendered (axes and hinting), and larger.
- `@fontsource` packages: they add a dependency and generated CSS, and still need licence files beside the fonts.
- Adding the latin-ext subsets: Indonesian and English UI text needs only Latin. The prototype also rendered without them, so characters outside Latin (→, ≈, ▲) fall back exactly as they did there.

### D2. Figures: tabular only where figures stack

- `body` uses `font-variant-numeric: lining-nums` with `--features-text` (`"lnum" 1, "cv11" 1`).
- Figures use `--features-num` (`"tnum" 1, "lnum" 1, "cv11" 1`): the role classes `.t-fig-l`, `.t-fig`, `.t-amt`, every primitive listed in SPEC §1, Tailwind `tabular-nums`, and every existing amount, count, time and table class. The task audits these: `.ws-tx-amount`, `.ws-kpi-value`, `.ws-tbl`, `.ws-cnt` and the others, and the module classes.
- The hero uses `--features-hero`; titles use `--features-display`.

Rejected alternatives:
- Global `tabular-nums`, as today: Inter's `tnum` widens the hyphen ("hari ke - 14").
- A monospace face for amounts: that was option 3's look, which the owner replaced with option 5's Inter figures.

### D3. Colour tokens: app names, option-6 values

The app's semantic names stay; their values change. Concepts the app lacks take the prototype's names. Values are light / dark.

| App token (today) | Option-6 source | Light | Dark |
| --- | --- | --- | --- |
| `--canvas`, `--window` | `--s0` | `#F8F7F4` | `#151517` |
| `--surface` (cards, panels) | `--s1` | `#F1EFEB` | `#1C1D20` |
| `--surface-2`, `--surface-3`, `--nav-hover`, `--tile`, `--accent-soft`, `--*-bg`, `--k-*-soft` | `--s2` | `#E6E3DD` | `#26272B` |
| `--nav-bg` | `--s1` | `#F1EFEB` | `#1C1D20` |
| new `--dialog` | `--dialog` | `#F8F7F4` | `#1C1D20` |
| `--line` | `--line` | `#E1DDD6` | `#2F3034` |
| `--line-strong` | `--line-strong` | `#847F78` | `#6E7076` |
| `--text`, `--ink` | `--ink` | `#1C1E22` | `#E8E7E4` |
| `--muted`, `--text-2`, `--tile-fg` | `--muted` | `#5B5E65` | `#A3A3A7` |
| `--ink-hover` | `--focus` (nearest) | `#45484F` | `#BDBEC2` |
| `--ink-fg` | `--s0` | `#F8F7F4` | `#151517` |
| `--accent` | `--accent` | `#FF334B` | `#FF4757` |
| `--accent-hover` | `--accent-hover` | `#FF4D61` | `#FF6675` |
| new `--accent-pressed` | `--accent-pressed` | `#FF6B7C` | `#FF8A95` |
| `--accent-ink` | `--on-accent` | `#0A0D14` | `#0A0D14` |
| `--accent-fg` | `--accent-text` | `#B8142D` | `#FF6B78` |
| `--in-fg`, `--ok-fg` | `--income` | `#056548` | `#34D399` |
| `--out-fg` | `--expense` | `#B8142D` | `#FF6B78` |
| `--tr-fg` | `--transfer` | `#075985` | `#38BDF8` |
| `--inv-fg` | `--invest` | `#5B21B6` | `#B39DFB` |
| `--warn-fg` | `--warning` | `#8F4F04` | `#FBBF24` |
| `--in-solid`, `--ok-dot`, `--k-saving`, `--flow-in`, `--use-tabungan` | `--m-income` | `#047857` | `#10B981` |
| `--tr-solid`, `--use-dana` | `--m-transfer` | `#0284C7` | `#0EA5E9` |
| `--inv-solid`, `--k-need`, `--flow-inv`, `--use-investasi` | `--m-invest` | `#7C3AED` | `#8B5CF6` |
| `--warn-solid`, `--flow-warn` | `--m-warning` | `#B45309` | `#F59E0B` |
| `--k-unexpected` | `--k-unexpected` | `#B45309` | `#FBBF24` |
| `--out-solid`, `--k-want` | `--m-expense` | `#E11D48` | `#FF4757` |
| `--k-culture` | `--k-culture` | `#0369A1` | `#38BDF8` |
| `--k-none`, `--flow-left`, `--flow-other`, `--use-harian` | `--k-none` | `#64748B` | `#94A3B8` |
| `--danger-solid` | `--danger` | `#B8142D` | `#E11D48` |
| `--danger-solid-hover` | `--danger` at 92 % brightness | `#A9122A` | `#CF1B42` |
| `--danger-contrast` | `--on-danger` | `#FFFFFF` | `#FFFFFF` |
| `--focus` | `--focus` | `#45484F` | `#BDBEC2` |
| new `--field-focus` | `--field-focus` | `#2B2D32` | `#CACACD` (prototype `#C9C9CC`, nudged to 3:1, owner 2026-11-05) |
| new `--ring` | `--ring` | `rgba(28, 30, 34, 0.10)` | `rgba(232, 231, 228, 0.12)` |
| `--scrim` | `--scrim` | `rgba(28, 26, 22, 0.44)` | `rgba(8, 8, 9, 0.62)` |
| `--shadow-float` | `--shadow-layer` | `0 16px 48px rgba(40, 34, 26, 0.14), 0 2px 6px rgba(40, 34, 26, 0.06)` | `0 16px 48px rgba(0, 0, 0, 0.5), 0 2px 6px rgba(0, 0, 0, 0.3)` |

Further rules:
- `--surface` maps to `--s1`, the prototype's panel tone, not `--s0`. Today's cards then read as option-6 context panels on the canvas, and no card vanishes before its page is rebuilt.
- Text on a semantic solid appears only on the primary (`--accent-ink`) and destructive (`--danger-contrast`) buttons after D9. `--*-solid-fg` and `--*-solid-hover*` are deleted once `tokens.test.ts` shows nothing references them; the same applies to `--shadow-card|xs|sm|md|window`.
- `--heat-*`, `--band-op` and `--kind-*` keep their values until phases 6 and 8, because their prototype replacements change the charts' structure, not just a colour.

Rejected alternatives:
- Renaming every colour token to the prototype's names (`--s0`, `--ink` …): this touches 500+ uses for no visual gain. The app's `--ink` already means a dark fill, so the rename would also invite errors.
- Keeping today's values for tokens the prototype lacks: that leaves cool greys beside warm surfaces.

### D4. Kakeibo, use and flow marks follow the prototype (owner choice A)

- Pillar colours become marks only: dots, bars and rails, always beside the pillar's name. Pillar text uses `--text` or `--muted`, never the pillar colour.
- `tokens.test.ts` changes:
  - the pillar rule becomes "each pillar mark at least 3:1 on the surfaces and tracks it sits on, `Belum diklasifikasi` neutral";
  - the distinct-hue and 4.5:1 pillar-text checks are removed;
  - the blue allow-list adds `--k-culture`.
- If the owner chooses B instead:
  - the `--k-*` rows of D3 keep today's values;
  - "Kakeibo Pillar Marks" in the tactile-pulse-ui delta takes back today's rule: distinct hues 25° from money hues and 40° apart, and 4.5:1 as text.

  The rest of the change is unaffected.

Rejected alternative: keeping the distinct hues silently. That would be a deviation from the accepted prototype without approval.

### D5. Type scale: seven sizes, three weights, guarded

- Tokens `--fs-12` … `--fs-44`.
- Role classes, copied from `styles.css`: `.t-hero`, `.t-fig-l`, `.t-fig`, `.t-amt`, `.t-title`, `.t-dtitle`, `.t-lead`, `.t-body`, `.t-strong`, `.t-meta`, `.t-label`.
- Body becomes 14/20 Inter 400.
- Tailwind `fontSize` is replaced, not extended, by the seven sizes with role line-heights:
  - keys `xs` 12/16, `sm` 14/20, `base` 16/24, `xl` 20/28, `2xl` 24/32, `3xl` 32/40, `4xl` 44/52;
  - `text-lg`, `text-5xl` and up are converted.
- Tailwind `fontWeight` keeps only `normal`, `medium` and `semibold`.
- Snap rule for existing CSS and arbitrary values:
  - sizes ≤ 12.5 → 12, 13–15.5 → 14, 16–18 → 16, 19–22 → 20, 23–28 → 24, 29–36 → 32, ≥ 40 → 44 (`clamp()` resolves to its 1440 px value first);
  - weights 300 → 400, 550 → 500, 650–900 → 600.
- Existing titles take their roles now: page `h1` → `.t-title` style, dialog titles → `.t-dtitle`, brand → Manrope 14/600. The hero role waits for its page.
- `typeScale.test.ts` fails on any other size, weight or family in `globals.css`, CSS modules, `tailwind.config.js` or class strings in `frontend/src`.

Rejected alternatives:
- Leaving sizes for the page phases: the audit's main finding (10–14 sizes per page) would stay through ten more phases, and the new fonts would land on half-pixel sizes.
- New Tailwind keys such as `text-role-body`: clearer names, but every use would be touched twice, here and again in the page phases that move to role classes.

### D6. Radii and spacing

- Three tokens with the prototype's names: `--r-control` 6, `--r-panel` 12, `--r-layer` 16. They replace the eleven `--radius-*` tokens, which are deleted; every use is touched anyway.
- Tailwind `borderRadius` is replaced by these keys:
  - `none` 0; `sm`, `DEFAULT` and `md` → 6; `lg` and `xl` → 12; `2xl` and `3xl` → 16;
  - `full` stays for true circles only (dots, unread marks).
- Arbitrary `rounded-[…]` values snap the same way. Pills (`--radius-pill` on badges, chips and switches) become 6, as SPEC §7 gives every control radius 6.
- Spacing tokens `--sp-4` … `--sp-64` are added for the primitives. Page paddings and gaps are not snapped in this phase, because that is layout.

Rejected alternative: keeping `--radius-*` names with new values. Eleven names for three values hides the rule.

### D7. Borders, fills and shadows

- Every border is 1 px:
  - `--line` for separators and the edges of tiles, dialogs, segments and key caps;
  - `--line-strong` for the edges of inputs, selects, secondary buttons and chips.
- All borders, outlines and rings thicker than 1 px or in a semantic or accent colour are removed. This includes the coral 2 px field focus in Catat, error borders in `--out-solid` and coloured card edges.
- Tinted semantic fills (`bg-*-solid/5` to `/20`, 109 uses in 18 files) become the badge fill `--surface-2`. Measured on the new surfaces they would fall below 4.5:1 for amber and coral text on `--s2` (4.15 and 3.89), and the prototype has none.
- The `color-mix` alpha mapping in `tailwind.config.js` goes away with them.
- Shadows remain only on dialogs, sheets, popovers, menus, the explanation popover and toasts (`--shadow-float`). `.ws-card` and controls lose theirs.
- `geometry.test.ts` fails on any `border-*-[2-9]`, `ring-[2-9]`, `outline` colour other than `--focus`, `border-color` from a semantic token, a tint utility, or `box-shadow` outside the floating layers and the field ring.

### D8. Focus: neutral and keyboard-only for controls, edge + ring for fields

- `:focus-visible` gives `outline: 2px solid var(--focus); outline-offset: 2px`. Rows, nav items and rail items use offset −2 px.
- Fields (input, select, textarea, search, the sentence field) use `:focus` with no outline, a 1 px `--field-focus` border and `box-shadow: 0 0 0 3px var(--ring)`, transitioning in 140 ms.
- Hover turns the field edge `--muted`.
- An error keeps the `--field-focus` edge, with the message in 12 px `--out-fg` and `aria-invalid` / `aria-describedby`.
- The 60 `focus-visible:ring-*` utilities are removed in favour of the global rule.
- Contrast: `--field-focus` vs `--line-strong` is 3.47:1 light and 3.00:1 dark, and `--focus` is at least 7:1 on every surface. `tokens.test.ts` measures both.
- This replaces BRIEF §16's "≥ 2 px" indicator for text fields only, by the owner's "simple and clean" direction.

Rejected alternative: a 2 px neutral outline on fields too. It is closer to BRIEF §16 but not what the owner accepted.

### D9. Primitives restyle in place; pages keep their layout

Components keep their APIs and DOM, so callers do not change. Their CSS takes the SPEC §7 dimensions.

| Today | Option 6 |
| --- | --- |
| `Button` primary (dark ink fill), `.ws-btn-ink`, `.ws-btn-accent`, green/lavender/sky solid buttons | Primary: h 36 (44 phone), padding 0 16, gap 8, radius 6, 14/500, `--accent` fill, `--accent-ink` text, hover `--accent-hover`, pressed `--accent-pressed` + 1 px down |
| `Button` secondary, `.ws-btn` | Secondary: transparent, 1 px `--line-strong`, hover `--surface`, pressed `--surface-2` |
| `Button` ghost, `.ws-linkbtn`, `.ws-accent-link` | Quiet (padding 0 12, `--muted`; small h 28 padding 0 8) or Link (`--accent-fg`, underline on hover, 3 px offset) |
| `Button` danger, `.ws-btn-danger` | Destructive: `--danger-solid`, white, hover 92 % brightness; quiet destructive in `--out-fg` |
| Semantic action pills ("Jawab", "Pasangkan", "Pilih") | Small quiet buttons; colour is for money meaning only |
| `.ws-icon-btn` | 36 × 36 (44 phone), radius 6, icon 16 |
| `Input`, `Select`, `FormField`, `.ws-field`, `.ws-search` | h 40, padding 0 12 (select right 36 with chevron `#847F78`), radius 6, `--dialog` fill in dialogs and `--window` on pages; money input h 56, 20/600 |
| `.ws-seg` | Track `--surface` + 1 px `--line`, padding 2, gap 2; items h 30 (40 phone); selected `--window` + 1 px `--line` hairline |
| `.ws-tab` | h 40 (44 phone), gap 24, 14/500 `--muted`; current `--text` + 2 px `--accent` bar on the line |
| `.ws-pill`, `Pill` | Badge: h 20, padding 0 8, radius 6, `--surface-2`, 12/500 semantic text |
| `.ws-chip` | Chip: h 28, padding 0 4 0 12, 1 px `--line-strong`, 12/500, remove button 24 × 24; quick chips h 32 (44 phone), `--surface` fill, 1 px `--line` |
| `.ws-kbd` | Key cap: h 20, min-w 20, padding 0 4, 1 px `--line`, 12/500 `--muted` |
| `.ws-switch` | 36 × 20, radius 6, knob 16 radius 6, off `--line-strong`, on `--accent-fg` |
| `.ws-menu`, `.ws-explain` | Radius 12, width 220–400, padding 8 0, items h 36 padding 0 16, hover `--surface-2` |
| `Modal`, `dialog.ws-dialog`, `.ws-drawer` | Radius 16 (sheet 16 16 0 0), 1 px `--line`, `--dialog`, header 24 24 16 with `.t-dtitle`, body 0 24 24, footer 16 24 with a top line |
| `.ws-toast` | Min-h 48, padding 8 8 8 16, gap 16, radius 12, `--dialog`, 1 px line, layer shadow |
| `.ws-skeleton`, `.ws-empty`, `.ws-error` | As SPEC §7: static `--surface-2` bars radius 6; empty 48 24 centred; error box 1 px `--line-strong`, radius 12 |

- A primitive's own height or padding may change. The page grid around it does not.
- A dialog's width class stays until its page phase maps it to 480/520/560/600/640/880.

Rejected alternatives:
- New primitive components beside the old ones: two button systems would live together for ten phases.
- Restyling pages in this phase: too large for one reviewable merge request, and against the plan's order.

### D10. Straight selection marks and group rails

- One shared style draws the selection bar as a pseudo-element with no radius:
  - 2 × 20 px, 8 px left of a nav or rail item's fill;
  - 2 px full height at x 0 on square rows.
- The group rail is a 2 px straight line beside a group.
- Both use `--accent` (bar) or the group's mark colour (rail).
- Existing curved left marks move to it: the goal account picker, the Grafik legend and the Tinjau rows. Their pages are not otherwise changed.
- `selectionMarks.test.ts` comes from the held branch `fix/selection-bars-and-tinjau` (64909ee). It fails on `border-left` or `box-shadow: inset … 0 0` on a selected or current state, and on `border-l-*` utilities.

### D11. Motion

- `--t-state` (140 ms) and `--t-layer` (200 ms) use `cubic-bezier(0.2, 0, 0, 1)`.
- Dialogs, popovers and toasts enter with opacity and a translateY of 8 px; sheets use 24 px.
- Everything is off under `prefers-reduced-motion: reduce`.
- `useAnimatedCounter` and its uses (Beranda) are removed, and figures render their final value. The tactile-pulse-ui "Animated Headline Figures" requirement is removed; SPEC §10 says "no counting numbers".

### D12. Verification tooling

- `scripts/verify/markas/` holds Node scripts that drive a local Chromium-based browser over CDP. They are adapted from the audit's `capture.mjs`, `measure.js` and `compare.mjs`:
  - capture any app or prototype route at 1440 × 900 and 390 × 844 in both themes;
  - extract the computed-style checklist;
  - run axe-core from `node_modules/axe-core`, already present through `vitest-axe`;
  - write a side-by-side image.
- `/komponen` renders every primitive and state in both themes, like the prototype's `#/komponen`. It exists only when the build sets `NEXT_PUBLIC_COMPONENT_SHEET=1`; otherwise it returns 404. Production builds never set it.
- `next.config.mjs` always inlines the flag (empty unless it is `1`), so the sheet's import is dead code elsewhere, and keeps the sheet's source out of the standalone output; `proxy.ts` answers `/komponen` with a 404 status when the flag is off, because a page-level `notFound()` rendered with status 200 under the app shell. The sheet's light cell uses `.theme-light`, which repeats the light tokens.

Rejected alternatives:
- Storybook or Playwright: new dependencies for a check the scripts already do.
- An always-on `/komponen` route: it would ship to production.

### D13. Theme mechanism unchanged

The `.dark` class, `THEME_SCRIPT`, the storage key and "Ikuti sistem" stay. The prototype's `data-theme` attribute is not adopted; only the values differ.

## Risks / Trade-offs

- An amount that relied on the global `tabular-nums` renders proportional. Mitigation: the D2 audit list, a test over the amount classes, and the checklist measuring `font-variant-numeric` on every element with a currency string.
- Size snapping moves text by 1–4 px in places. Mitigation: before and after captures of every route; section order, grids and widths must stay identical.
- Coral becomes the only primary fill, so a page may show more than one coral button until its phase reduces visible actions to two. This is accepted for the interim and listed for the owner.
- The interim look combines option-6 type and colour with the old layouts until phases 2 to 11 land (plan decision 3).
- Mechanical edits across about 50 files can miss a case. Mitigation: guard tests fail on the patterns; the browser gate measures the rendered page.

## Migration and rollback

There is no data or API migration. Theme and layout preferences in browser storage keep their keys. Rollback is a revert of the merge commit: the old font files and tokens return with it.

## Open Questions

None. On 2026-11-05 the owner chose D4 option A (the prototype's marks) and D12 option A (the development-only `/komponen` sheet); see the proposal's Approval section. The prototype-data parity seed of plan decision 7 arrives with phase 2, as the plan's tooling section sets out.
