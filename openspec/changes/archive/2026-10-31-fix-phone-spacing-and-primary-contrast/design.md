# Design

## Context

See proposal.md for the problem. Facts that shape the approach:

- 1ae5c4a (2026-05-28) introduced the spacing scale in `tokens.css` (`--space-3xs` 0.25rem through `--space-3xl` 7rem, with `--space-sm: 1rem` and `--space-md: 1.5rem`) together with the `.workbench-page` and phone `.p-5` rules. a880522 (2026-09-15) rewrote `tokens.css` without the scale and kept the rules.
- Tailwind 3 emits utilities at `@tailwind utilities`, before the custom rules in `globals.css`, so the phone `.p-5` rule wins over `sm:p-6`/`sm:p-7` between 640 and 900 px as well. Today all 15 `p-5` elements on the seven pages measure 0 px padding at 390 and 320 px (browser evidence, before state).
- `.workbench-page` is not used by any component; only the rule exists.
- The approved main-workspace gutter in `executive-sidebar-layout` is `px-4 sm:px-6 lg:px-8`; `AppLayout` currently uses `px-3` (12 px) on phones since c3b36c8.
- `--primary-contrast` is `#FFFFFF` in both themes and feeds `.btn-primary`, the top-bar "+ Catat" button and the active sidebar items. The shared `Button` primary variant hard-codes `text-white`. The Quick Capture and internal-movement submit buttons switch to `bg-[var(--primary)] text-[var(--text)]` for their brief success state, which is light ink on coral in dark mode.
- `--primary-hover` is used only by `.btn-primary:hover`, `Button` primary hover and the "+ Catat" hover.

## Decisions

1. **Restore `--space-sm: 1rem` and `--space-md: 1.5rem` in `:root`.** These are the values the rules were written against. On phones `.p-5` cards get 16 px inner padding; `.workbench-page` gets 24 px on desktop and 16 px at 900 px and below. Only the two referenced tokens are restored; the unused rest of the old scale stays out because nothing uses it. Rejected: pointing the rules at literal values (loses the token the rule names), and restoring the whole nine-step scale (adds unused tokens).
2. **Leave the `AppLayout` main gutter at `px-3`.** Changing it moves every page on phones and is outside the approved defect. It is recorded under "Open decisions for owner".
3. **Dark ink `#0A0D14` for `--primary-contrast` in both themes.** It is the light theme's `--color-ink`. It is written as a literal in `.dark` because `--color-ink` is light there. Ratios: 5.39:1 on `#FF334B` (light), 5.82:1 on `#FF4757` (dark). Rejected: pure black (5.83/6.29, harsher than the existing ink for no AA gain) and darkening the coral (the owner chose to keep the coral).
4. **Light `--primary-hover` `#E02038` → `#F02A42`.** Dark ink on `#E02038` is 4.10:1 and even pure black is 4.43:1, so the old hover cannot carry dark text at AA. `#F02A42` keeps the "slightly deeper coral on hover" direction and gives 4.73:1. The dark theme hover `#FF6675` already gives 6.86:1 and is unchanged. Rejected: a lighter light-theme hover such as `#FF5266` (6.15:1), which reverses the established light-theme hover direction; and switching the text back to white on hover (text colour flicker).
5. **Every text on a `--primary` background uses `--primary-contrast`.** `Button` primary variant: `text-white` → `text-[var(--primary-contrast)]`. Quick Capture and internal-movement success state: `text-[var(--text)]` → `text-[var(--primary-contrast)]` (3.19:1 → 5.82:1 in dark). Icons inherit `currentColor`. The `.btn-primary` rule drops its `#ffffff` fallback, because the token is always defined and a white fallback would fail AA.
6. **"+ Catat" shortcut badge `bg-black/20 text-white/90` → `bg-white/30 text-primary-contrast`.** Dark ink on the badge measures 7.30 (light), 6.66 (light hover), 7.96 (dark) and 9.24:1 (dark hover). The old badge measured 4.57, 5.08, 4.28 and 3.76:1.
7. **Disabled and focus states are unchanged.** Disabled buttons keep `opacity: 0.5` (WCAG 1.4.3 exempts inactive controls). The focus ring uses `--color-focus` (coral) around the button; against the page it measures 3.03:1 (light paper) and 6.07:1 (dark paper), meeting the 3:1 non-text minimum, and it does not depend on the text colour.
8. **Regression test in `src/app/tokens.test.ts`.** It parses `tokens.css` and `globals.css`, treats the `next/font` variables declared in `layout.tsx` as defined, and fails when a stylesheet uses an undefined custom property. A short explicit list holds the three pre-existing `--color-accent*` gaps behind `.card-green`; the test also fails when one of them becomes defined, so the list only shrinks. It resolves `--primary`, `--primary-hover` and `--primary-contrast` for `:root` and `.dark` and asserts at least 4.5:1 for each pair.

9. **Goal card header may shrink its title group.** At 320 px the restored 16 px padding pushed the archive button of the "Dana Darurat" goal card fully outside the card (it was already half clipped before). The title group gets `min-w-0` and the action group `shrink-0`, so the goal name truncates further and both 44 px actions stay inside the card. Rejected: wrapping the header onto two lines (changes the card's layout at every width).

## Risks / Trade-offs

- [Phone cards gain 16 px padding and become 32 px narrower inside] → Browser check at 390 and 320 px for horizontal overflow and clipped content on all seven pages, both themes.
- [Between 640 and 900 px, `sm:p-6` cards get 16 px instead of 24 px, as the original rule intended] → Accepted; this is the rule's documented intent and is better than today's 0 px.
- [Light hover `#F02A42` is only 1.14:1 away from the rest coral, so the hover is subtler] → The pointer cursor and `.btn-primary`'s brightness filter still mark hover; hover distinction is not an AA requirement.

## Migration Plan

Frontend-only. Ships with the normal frontend release. Rollback is reverting the commit.

## Open decisions for owner

- **Phone main gutter.** `executive-sidebar-layout` states `px-4` (16 px) for the main workspace; `AppLayout` uses `px-3` (12 px) on phones. Aligning it is a one-class change that shifts every phone page by 4 px per side. Left unchanged here.
- **Danger and rose buttons.** `Button` danger variant (white on `--danger`, 3.60:1 light, 3.34:1 dark) and `bg-rose-600 text-white` actions such as "Tagihan Baru" (4.74:1 light, 2.83:1 dark on `#FF6675`) fail AA in dark. Not primary buttons, so out of scope.
- **Other undefined custom properties.** `--color-accent`, `--color-accent-2`, `--color-accent-ink` (`.card-green`), and in components `--accent-lime`, `--danger-light`, `--foreground`, `--info`, `--info-light`, `--primary-light`, `--warning`, `--warning-light`.
- **Unused `.workbench-page` rule.** Valid again after this change but referenced by no component.
