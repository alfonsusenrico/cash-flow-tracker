## 1. Baseline and tooling

- [x] 1.1 Record the owner's plan approval and the answers to the Kakeibo marks (D4) and `/komponen` (D12) choices, with the date, in `proposal.md` and `PROJECT_STATE.md`. Evidence: both files name the approval date and the chosen options.
- [x] 1.2 Version the capture, measure and compare scripts under `scripts/verify/markas/`, adapted from the audit: CDP capture of app and prototype routes at 1440 × 900 and 390 × 844 in both themes; the computed-style checklist; axe-core from `node_modules`; side-by-side images. Evidence: a dry run over the prototype's `#/komponen` and `#/beranda` writes captures and a checklist JSON. No new npm dependency (`git diff package.json package-lock.json` empty).
- [x] 1.3 Capture the "before" baseline of every current route and key dialog with the audit's synthetic seed. Evidence: captures and the checklist JSON in `research/evidence/apply-markas-foundation/before/`, tied to `origin/main` `18a4477`.

Expected Result: the gate can measure prototype and app in the same way, and the pre-change state is recorded.

## 2. Fonts and figures

- [x] 2.1 Download the Latin variable `woff2` files that the prototype's Google Fonts URL serves for Inter and Manrope, and the OFL texts from `google/fonts`. Record SHA-256, `fvar` axes and the presence of the `tnum`, `lnum` and `cv11` features (fonttools in a throwaway uv environment outside the repository). Evidence: the values in `verification.md`; the files under `frontend/src/app/fonts/`.
- [x] 2.2 Load both through `next/font/local`, Inter with weight `400 600` and Manrope with `600`. Remove the Plus Jakarta Sans and Geist Mono files and references, and set `--font-sans`, `--font-display` and `font-synthesis-weight: none` (D1). Evidence: an updated `fonts.test.ts` covers the "Build without access to a font provider" and "Page renders with bundled fonts" scenarios; `npm run build` with the network to Google blocked; the CSP test unchanged and passing.
- [x] 2.3 Set body figures to `--features-text` and scope tabular figures to the role classes, primitives and every existing amount, count, time and table class (D2). Evidence: the class list in `verification.md`; a browser check that every element whose text matches `Rp` or a time has `font-variant-numeric` containing `tabular-nums`, while the Beranda subtitle "hari ke-…" does not.

Expected Result: every page reads in Inter, titles in Manrope, amounts line up, and no font leaves the origin.

## 3. Tokens

- [x] 3.1 Set the colour tokens to the D3 values in `:root` and `.dark`, add `--dialog`, `--accent-pressed`, `--field-focus` and `--ring`, and apply D4 (or keep the pillar values if the owner chose B). Evidence: `tokens.test.ts` asserts every D3 value per theme ("Light theme values", "Dark theme values"), the contrast pairs (text 4.5:1, marks 3:1, `--field-focus` against `--line-strong` 3:1, `--focus` 3:1) and the pillar-mark rule.
- [x] 3.2 Add `--fs-*`, the `--features-*` tokens, `--sp-*`, `--r-control|panel|layer`, `--t-state|layer` and the role classes from `styles.css`. Replace the `--radius-*` and the card, xs, sm, md and window shadow tokens (D5, D6, D7). Evidence: the token test "defines every custom property the stylesheets and components use" passes with the old names gone.
- [x] 3.3 Replace Tailwind's `fontSize`, `fontWeight`, `borderRadius` and `boxShadow` scales, keep the colour keys on the tokens, and drop the tint `color-mix` mapping and the mono family. Evidence: `tailwindTheme` assertions in `tokens.test.ts`; `npm run build`.

Expected Result: one token set carries every option-6 value in both themes, guarded by tests.

## 4. Scale, geometry and borders across the app

- [x] 4.1 Snap every font size and weight in `globals.css`, the CSS modules and the class strings to the D5 rule; page titles, dialog titles and the brand take their Manrope roles. Evidence: a new `typeScale.test.ts` covers "A stray size or weight is added"; the browser checklist "Every page measured" finds only the seven sizes and three weights on every route.
- [x] 4.2 Snap every radius to 0, 6, 12, 16 or a circle. Remove borders over 1 px and coloured borders, outlines and rings, card and control shadows, and the 109 tinted fills, which become the badge fill (D6, D7). Evidence: a new `geometry.test.ts` covers "A thick or coloured border is added" and "No tinted semantic fills"; the browser checklist finds no computed border ≥ 2 px, no coloured border on a rounded element and `box-shadow: none` on cards ("Cards are flat").
- [x] 4.3 Replace pillar-coloured text with neutral text and a pillar mark (D4). Evidence: a component test for the Beranda and Analisis Kakeibo rows; the browser check "Pillars on Analisis, Beranda and Laporan".

Expected Result: the whole app renders with seven sizes, three weights, three radii, 1 px neutral lines and flat cards, while page layouts are unchanged.

## 5. Shared primitives

- [x] 5.1 Restyle the buttons to D9 (primary coral, secondary, quiet, small quiet, link, destructive, icon), including the former ink, green, sky and lavender solid buttons and the semantic action pills. Evidence: updated `Button` and primitive tests; a browser check of "Primary button text contrast", "Keyboard focus on a button", "Small row actions are quiet" and "Tactile button press feedback" in both themes.
- [x] 5.2 Restyle the fields to D8 and D9: input, select with its chevron, textarea, search, money input and FormField errors. Evidence: `FormField` and `QuickCaptureModal` tests for `aria-invalid` and `aria-describedby`; a browser check of "Clicking into the sentence field" and "Field error stays neutral" (1 px `#2B2D32` edge + 3 px ring and no outline in light; `#C9C9CC` in dark).
- [x] 5.3 Restyle the segmented control, tabs, badge, chip, quick chip, key cap, switch, checkbox, menu and popover, explanation popover, dialog, sheet, toast, scrim, skeleton, empty and error states to SPEC §7 (D9). Evidence: the `/komponen` sheet captured beside the prototype's `#/komponen` at 1440 in both themes, with a per-primitive checklist (height, padding, radius, border, font, colours at rest, hover, pressed, selected, focus, disabled and error) and every item matching.
- [x] 5.4 Remove every `focus-visible:ring-*` utility in favour of the global neutral outline (D8). Evidence: `geometry.test.ts` forbids the utilities; a keyboard pass on Beranda, Transaksi, Catat and Pengaturan shows the 2 px neutral outline on every stop, and the pointer check "Pointer click on a button" shows none.

Expected Result: every shared control matches the prototype's component sheet in both themes.

## 6. Selection marks and motion

- [x] 6.1 Add the shared selection bar and group rail and the `selectionMarks.test.ts` guard (taken over from 64909ee with option-6 geometry). Move the goal account picker, the Grafik legend and the Tinjau rows to it (D10). Evidence: the guard covers "A curved mark is added again"; a component test covers "Chosen pocket in the goal account picker".
- [x] 6.2 Add the motion tokens to primitives and layers, turn every transition off under reduced motion, and remove `useAnimatedCounter` and its use (D11). Evidence: Beranda tests for "A figure changes"; a browser check of "Reduced motion" with emulated `prefers-reduced-motion: reduce`; `rg useAnimatedCounter frontend/src` is empty.

Expected Result: selection is straight everywhere it is marked, and nothing counts or bounces.

## 7. Verification and delivery

- [x] 7.1 Run the common gate of the plan:
  - every current route and key dialog at 1440 and 390 in both themes, with the global rules (families, sizes, weights, borders, radii, shadows), no horizontal overflow at 1440, 390 and 320, axe-core zero serious or critical, and zero console errors;
  - figure parity against the 1.3 baseline;
  - before and after captures showing unchanged section order, grids and widths.

  Evidence: counts and the side-by-side images in `verification.md` and `research/evidence/apply-markas-foundation/`.
- [x] 7.2 Run `npm test`, `npm run type-check`, `npm run lint`, `npm run build`, `git diff --check` and `openspec validate --all --strict`, and confirm `git diff --stat origin/main -- backend db mobile nginx` is empty. Review the final diff for unrequested changes. Evidence: command results with counts in `verification.md`; the backend suite is recorded as not run because no backend file changed.
- [x] 7.3 Present the evidence to the owner. After the owner accepts, archive the change, push `feat/markas-foundation` and report the compare link; the owner opens and merges the merge request. Evidence: the `openspec archive` output, the pushed branch and the compare link.

Expected Result: the foundation is verified against the prototype and handed to the owner for review.
