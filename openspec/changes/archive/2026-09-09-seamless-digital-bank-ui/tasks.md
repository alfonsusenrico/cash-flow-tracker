## 1. Modern Bauhaus Typography (`Outfit`) Integration

- [x] 1.1 Configure `next/font/google` with `Outfit` and `Geist_Mono` in `frontend/src/app/layout.tsx` to inject `--font-outfit` and `--font-geist-mono` into `<html>` and `<head>`.
- [x] 1.2 Update `frontend/src/app/globals.css` and `frontend/tailwind.config.js` to set `Outfit` as the universal root and default sans/display font.
- [x] 1.3 Lock `frontend/tokens.css` default root variables to use `var(--font-outfit), 'Outfit', sans-serif`.

## 2. Complete Removal of Preview Theme Switcher

- [x] 2.1 Remove `<StyleSwitcher />` from `frontend/src/components/layout/TopBar.tsx`.
- [x] 2.2 Remove `<StyleSwitcher />` from `MobileDrawer` in `frontend/src/components/layout/Sidebar.tsx`.
- [x] 2.3 Remove "Design System Style" section and preset imports from `frontend/src/components/ui/SettingsModal.tsx`.
- [x] 2.4 Remove preset state and handlers from `frontend/src/components/layout/AppLayout.tsx`.
- [x] 2.5 Delete `frontend/src/components/ui/StyleSwitcher.tsx`.
- [x] 2.6 Clean up unused legacy preset overrides from `frontend/tokens.css`.

## 3. Verification & Testing

- [x] 3.1 Run frontend type-check (`npm run type-check`) and linter (`npm run lint`).
- [x] 3.2 Run frontend production build (`npm run build`).
- [x] 3.3 Rebuild and restart Docker frontend container (`docker compose up -d --build frontend`).
- [x] 3.4 Validate OpenSpec change integrity with `openspec validate seamless-digital-bank-ui`.
