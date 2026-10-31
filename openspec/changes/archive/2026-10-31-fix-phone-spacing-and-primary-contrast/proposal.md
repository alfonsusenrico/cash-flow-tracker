# Proposal

## Why

`frontend/src/app/globals.css` pads `.workbench-page` with `var(--space-md)` and, at 900 px and below, pads `.workbench-page` and `.p-5` with `var(--space-sm)`. Neither custom property is defined since the clean-slate refactor (a880522, 2026-09-15) replaced `tokens.css` and dropped the spacing scale while keeping these rules. An undefined `var()` makes the declaration invalid at computed-value time, so every `p-5` card (Beranda, Analisis, Rekening & Saldo, Target & Tagihan, the shared `Card` with `padding="lg"`) renders with zero inner padding on phones and narrow tablets: text and progress bars touch the card edge.

Separately, the coral primary buttons ("Simpan profil", "Simpan anggaran", "Hubungkan ponsel", the top-bar "+ Catat", the active sidebar item) carry white text that measures 3.60:1 on `#FF334B` in light and 3.34:1 on `#FF4757` in dark, below the WCAG 2.2 AA 4.5:1 minimum for text.

## Approval

On 2026-10-31 the owner approved fixing the undefined spacing tokens, and said "ok" to the coordinator's recommendation to give the coral primary buttons dark text so they meet WCAG AA while keeping the coral colour. Recorded 2026-10-31.

## Expected Outcome

- On a phone (390 px and 320 px), every `p-5` card has 16 px of inner padding again; no page scrolls horizontally and no content is newly clipped.
- On desktop (1280 px and 1440 px), every page looks as it did before.
- Every coral primary button and the active coral sidebar item show dark text and icons that measure at least 4.5:1 against the coral, at rest and on hover, in light and dark. The coral itself is unchanged at rest.

## What Changes

- `frontend/tokens.css` defines `--space-sm: 1rem` and `--space-md: 1.5rem`, the values these rules were written against in 1ae5c4a.
- `--primary-contrast` becomes the dark ink `#0A0D14` in both themes, and the light theme's `--primary-hover` moves from `#E02038` to `#F02A42` so dark text still meets 4.5:1 on hover.
- The shared `Button` primary variant, the "+ Catat" shortcut badge and the transient success state of the Quick Capture and internal-movement submit buttons use `--primary-contrast` instead of hard-coded white or `--text`.
- The goal card header lets its title shrink so the edit and archive buttons stay inside the card at 320 px once the padding returns.
- A token unit test fails when a stylesheet uses a custom property that is never defined, and when primary text falls below 4.5:1 on the primary or primary-hover colour in either theme.

## Non-goals

- Restyling anything else: the main content gutter (`px-3`, 12 px on phones, set in `AppLayout`), other components, the coral at rest, danger and rose buttons, layout and radii stay as they are. Pre-existing gaps found along the way are listed in design.md as follow-ups.
- Production access or release. Verification is local with synthetic data. Pushing the branch is authorised; opening and merging a pull request stays with the owner, and release uses the approved pipeline.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `theme-presets-switcher`: "Modern Button Styling System" states that primary action text meets WCAG 2.2 AA 4.5:1 against the primary colour at rest and on hover in light and dark.

The spacing fix restores existing behaviour and changes no requirement.

## Impact

- Frontend only: `frontend/tokens.css`, `frontend/src/app/globals.css`, `frontend/src/components/ui/Button.tsx`, `frontend/src/components/layout/TopBar.tsx`, `frontend/src/components/ui/QuickCaptureModal.tsx`, `frontend/src/components/ui/InternalMovementModal.tsx`, `frontend/src/app/goals/page.tsx`, and a new `frontend/src/app/tokens.test.ts`.
- No API, schema, backend, Android or dependency change. Capabilities of the project are unchanged; the `ui-ux-quality` standard applies.
