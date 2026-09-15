## 1. Design System Tokens & Typography Presets

- [x] 1.1 Add Google Fonts import for Geist, Plus Jakarta Sans, Outfit, and JetBrains Mono in `frontend/src/app/globals.css`.
- [x] 1.2 Implement multi-preset CSS variables in `frontend/tokens.css` for `swiss`, `nordic`, `bauhaus`, and `stripe` (colors, borders, fonts, button radii).
- [x] 1.3 Add button utility classes with tactile press states and adaptive corner radii (`var(--radius-btn)`).

## 2. Global Preset State & Live Switcher Component

- [x] 2.1 Update `frontend/src/components/layout/AppLayout.tsx` with `preset` state (`swiss` | `nordic` | `bauhaus` | `stripe`), `localStorage` persistence, and `data-preset` DOM synchronization.
- [x] 2.2 Add interactive Style Switcher selector pill / dropdown in `frontend/src/components/layout/TopBar.tsx`.
- [x] 2.3 Add Style Preset picker in `frontend/src/components/ui/SettingsModal.tsx` and mobile navigation.

## 3. Organized Transaction Entry Form & Polished Buttons

- [x] 3.1 Refactor `frontend/src/components/ui/QuickCaptureModal.tsx` to replace shattered category chip clusters with an organized category select dropdown with icons and clean borders.
- [x] 3.2 Update form action buttons in `QuickCaptureModal.tsx` with modern primary and secondary tactile buttons.
- [x] 3.3 Ensure transaction type pills (Cash Out, Cash In, Transfer) use clean modern segmented control styling.

## 4. Verification & Testing

- [x] 4.1 Run frontend type-check (`npm run type-check`) and linter (`npm run lint`).
- [x] 4.2 Run frontend production build (`npm run build`).
- [x] 4.3 Rebuild and restart Docker frontend container (`docker compose up -d --build frontend`).
- [x] 4.4 Validate OpenSpec change integrity with `openspec validate modern-presets-and-clean-forms`.
