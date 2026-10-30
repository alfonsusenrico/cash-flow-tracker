## 1. Design System & Typography Foundation

- [x] 1.1 Configure `Plus_Jakarta_Sans` in `frontend/src/app/layout.tsx` and map `--font-jakarta` in `globals.css` and `tokens.css`.
- [x] 1.2 Update `tokens.css` with electric pop colors (Electric Mint `#00D09C`, Neon Rose `#FF3B69`, Candy Violet `#6366F1`, Amber `#FFB020`) and candy-tint translucent washes.
- [x] 1.3 Add tactile utility classes in `globals.css` (`.pressable`, `.card-squircle`, `.pill-chip`) with spring compression physics.
- [x] 1.4 Map `var(--font-jakarta)` into `frontend/tailwind.config.js` `sans` and `display` so Tailwind utility classes resolve to Plus Jakarta Sans.

## 2. Motion System & Micro-Interactions

- [x] 2.1 Implement `frontend/src/hooks/useAnimatedCounter.ts` for smooth ease-out number rolling without layout shifts.
- [x] 2.2 Implement sliding indicator physics on segmented pill controls in `frontend/src/components/ui/QuickCaptureModal.tsx`.
- [x] 2.3 Add transaction success micro-celebration checkmark state (`✓ Tersimpan`) to the Quick Capture modal before dismiss.
- [x] 2.4 Add smooth animated width/ring progress fills on category budget progress bars and goal targets.

## 3. UI Component & Page Makeover

- [x] 3.1 Upgrade Pulse page (`frontend/src/app/page.tsx`) hero card with candy glow tints, large Plus Jakarta Sans typography, and rolling allowance counter.
- [x] 3.2 Refresh Insights page (`frontend/src/app/insights/page.tsx`) burn rate tiles with squircle geometry, pop accents, and animated projection numbers.
- [x] 3.3 Refresh Accounts page (`frontend/src/app/accounts/page.tsx`) with tactile card elevations and bouncy transfer trigger.
- [x] 3.4 Transform `frontend/src/app/page.tsx` from the legacy asymmetric 12-column "Cockpit" dashboard into a modern consumer fintech experience (Bibit / Cashew / Kazz inspired) with dynamic time greetings, full-width hero canvas, fuel gauge, and interactive 3-pillar Kakeibo cards (`KakeiboPillarCards.tsx`).

## 4. Verification & Quality Assurance

- [x] 4.1 Run `npm run type-check`, `npm run lint`, and `npm run build` in frontend.
- [x] 4.2 Rebuild and restart Docker containers, and verify visual and interaction fidelity in browser.
