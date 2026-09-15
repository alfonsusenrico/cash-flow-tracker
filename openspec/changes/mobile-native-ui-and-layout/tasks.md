# Tasks: Mobile-Native UI & Handheld Resolution Layout

## Phase 1: Layout Shell & Bottom Navigation Polish
- [ ] 1.1 Update `frontend/src/components/layout/TopBar.tsx` to streamline the mobile top bar (remove redundant hamburger toggle, keep compact title, statement month pill, and privacy toggle).
- [ ] 1.2 Update `frontend/src/components/layout/AppLayout.tsx` to conditionally render `Sidebar` strictly on `lg:` viewports, eliminating the obsolete `MobileDrawer` slide-over.
- [ ] 1.3 Refactor `frontend/src/components/layout/BottomNav.tsx` with enhanced touch ergonomics: 4 primary navigation targets (`Beranda`, `Transaksi`, `Analisis`, `Dompet`), prominent center `+` action button with tactile press feedback, and proper safe-area inset handling (`pb-safe`).

**Expected Result:** A clean mobile navigation frame with zero duplicate menus, instant bottom navigation switching, and an uncluttered top bar.
**Verification Evidence:** Visual inspection in mobile viewport (<1024px) confirms hamburger icon is gone, top bar is sleek, and bottom navigation operates smoothly.

## Phase 2: Mobile Bottom-Sheet Quick Capture
- [ ] 2.1 Refactor `frontend/src/components/ui/Modal.tsx` to support a responsive bottom-sheet presentation mode on mobile screens (`rounded-t-3xl`, bottom-0 positioning, drag handle bar) while retaining standard centered dialogs on desktop viewports (`md:` and above).
- [ ] 2.2 Enhance `frontend/src/components/ui/QuickCaptureModal.tsx` for mobile touch: ensure large tabular figures display, thumb-friendly numeric keypad, 1-tap fast amount chips (`+10rb`, `+50rb`, `+100rb`), and horizontally scrollable category chips in the lower reach zone.

**Expected Result:** Tapping the center `+` button on mobile slides up a bottom sheet that is easily operable with one thumb without needing the system software keyboard for simple numbers.
**Verification Evidence:** Quick capture opens as a slide-up drawer on mobile and a centered modal on desktop; numeric input and category selection work seamlessly.

## Phase 3: Dedicated Mobile Home View (`MobileHomeView`)
- [ ] 3.1 Create a dedicated `<MobileHomeView />` component in `frontend/src/components/dashboard/MobileHomeView.tsx` (or integrated into `frontend/src/app/page.tsx` with clear `lg:hidden` separation):
  - Compact Hero Balance card with 1-tap toggle between Liquid and Invested assets.
  - 3 primary thumb action buttons: `[ + Catat ]`, `[ ⇄ Pindah ]`, `[ ⚡ Alokasi ]`.
  - Daily safe-to-spend allowance badge with spending velocity indicator.
  - Single-row segmented Kakeibo 50/30/20 pace bar (Kebutuhan / Keinginan / Tabungan).
  - Immediate chronological Recent Transactions timeline positioned directly below the pulse fold.
- [ ] 3.2 Ensure the existing desktop cockpit layout is isolated to `hidden lg:block`, preserving all desktop cards, multi-column grids, and analytical modules without regression.

**Expected Result:** On mobile screens, the user immediately sees balance, safe daily allowance, budget pace, and recent transactions without scrolling through 12 vertical cards.
**Verification Evidence:** In mobile view (390px), the home view displays the compact pulse fold and recent transactions above secondary details; in desktop view (1440px), the multi-column command center remains identical.

## Phase 4: Mobile Ledger & Accounts Experiences
- [ ] 4.1 Update `frontend/src/app/ledger/page.tsx` for mobile: render sticky date headers (`Hari Ini`, `Kemarin`, `DD MMMM YYYY`), horizontal category and type filter pills, and ensure clicking any transaction opens the detail drawer in bottom-sheet mode.
- [ ] 4.2 Update `frontend/src/app/accounts/page.tsx` for mobile: optimize account cards for touch with clear parent-pocket nesting (Bank Jago Kantong), balance badges, and a quick transfer bottom sheet.

**Expected Result:** Transaction history and account management feel like native mobile banking experiences with date-grouped feeds and touch-friendly cards.
**Verification Evidence:** Ledger transactions are grouped by date with sticky headers on mobile; tapping opens the transaction drawer; accounts list is compact and shows pockets cleanly.

## Phase 5: Verification & Contract Validation
- [ ] 5.1 Run frontend type checks (`npm run type-check`) and linting (`npm run lint`) to ensure zero errors.
- [ ] 5.2 Run frontend production build (`npm run build`) to verify clean compilation.
- [ ] 5.3 Validate OpenSpec change integrity with `openspec validate mobile-native-ui-and-layout`.

**Expected Result:** All automated checks, builds, and OpenSpec validations pass with zero errors or warnings.
**Verification Evidence:** Clean terminal outputs from `type-check`, `lint`, `build`, and `openspec validate`.
