# Proposal

## Why

`fix-web-tokens-and-spending-so-far` left two light-theme contrast defects as open decisions for the owner:

- Spring Lime `#66CC55` is used as link text on phone Beranda ("Kelola →", "Lihat Semua →"). It measures 1.71:1 on the light paper, where WCAG 2.2 AA needs 4.5:1 for text.
- Coral `#FF334B` is the colour of every expense amount, the "Uang Keluar" badge, coral links such as "+ Tambah Tagihan" and the sidebar avatar initials. It measures 3.60:1 on white, 3.03:1 on the paper and 2.68:1 on the raised surface. `text-rose-600/700` (`#E02038`) passes only on white (4.74:1) and falls to 3.52:1 on the raised surface.

A browser run on the current main (d3ed9f1) found 50 of 77 coral text elements and 2 of 11 lime elements below their AA minimum in the light theme. The dark theme passes everywhere (lowest 5.00:1).

## Approval

On 2026-10-31 the owner approved the colour tweaks ("do the colour tweaks too"), relayed by the coordinator. Recorded 2026-10-31.

## Expected Outcome

- In the light theme, every lime and coral text on the seven primary pages measures at least 4.5:1 against the surface it sits on, at 1280 and 390 px. Lime icons that carry meaning measure at least 3:1.
- The dark theme still measures at least 4.5:1 for the same elements.
- Lime pills, the lime chart peak, coral fills, coral buttons and coral chart marks look exactly as before.
- Expense amounts still read as coral/rose and links still read as lime; no colour changes meaning.

## What Changes

- `frontend/tokens.css` adds text tokens: `--accent-text` (darker lime `#2D6E22` in light, `#66CC55` in dark), `--expense-text` (darker coral `#B8142D` in light, the existing `#FF4757` in dark), `--primary-text` (the same pair for brand-coral text) and `--danger-text`, which follows `--expense-text`.
- `frontend/tailwind.config.js` maps the text utilities of the coral scales (`text-rose-400` to `text-rose-700`, `text-expense`, `text-primary`) to the text tokens. Background, border, ring and shadow utilities keep the vivid fill colours.
- Arbitrary-value text classes use the tokens: the two lime links on phone Beranda use `--accent-text`; the error and negative texts that used `--danger` use `--danger-text`; the unused `Badge` and `MetricTile` coral text uses `--primary-text` and `--danger-text`.
- `tokens.test.ts` asserts at least 4.5:1 for each text token on the paper, surface and raised-surface tokens in both themes, and checks that the Tailwind text utilities resolve to the text tokens while the fills keep the fill tokens.

## Non-goals

- Changing any fill: `--accent-lime`, `--color-expense`, `--primary`, the lime nav pill, chart colours and coral buttons stay as they are.
- The lime "+" icons on the charcoal quick-action buttons: they measure 8.07:1 and keep `--accent-lime`.
- Other colour families (emerald income, violet transfer, amber, sky), user-chosen category colours, and the unused `.kakeibo-chip-want` rule.
- Production access or release. Verification is local with synthetic data. Pushing the branch is authorised; opening and merging a pull request stays with the owner, and release uses the approved pipeline.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `tactile-pulse-ui`: "Anti-AI-Slop Visual Design Standard" states that lime and coral text uses the text tokens and meets 4.5:1 in light and dark, while fills keep the signature colours.

## Impact

- Frontend only: `frontend/tokens.css`, `frontend/tailwind.config.js`, `frontend/src/components/dashboard/MobileHomeView.tsx`, `frontend/src/components/ui/Input.tsx`, `frontend/src/components/ui/DevicePairingDialog.tsx`, `frontend/src/components/ui/Badge.tsx`, `frontend/src/components/ui/MetricTile.tsx` and `frontend/src/app/tokens.test.ts`. `AGENTS.md` points at `tokens.css` as the design source and needs no change.
- No API, schema, backend, Android or dependency change. The project capabilities are unchanged; the `ui-ux-quality` standard applies.
