## Context

The cash flow tracker currently implements responsive styling primarily by shrinking columns and stacking elements vertically on mobile viewports. On smartphone screens (360px–430px wide), this results in an uncomfortably tall single column where users must scroll through multiple desktop-sized cards (Hero Card, Daily Budget, Narrative, Kakeibo Cards, Metric Ribbon, Charts) before reaching recent transactions or accounts.
The user requested a complete review and audit of the UI design and layout to prepare for true mobile resolution support—recreating the design and layout specifically for mobile devices rather than merely resizing desktop components.

## Goals / Non-Goals

**Goals:**
- **Zero-Scroll Primary Pulse:** On mobile, place the core status (Available balance, Today's safe allowance, Kakeibo pace bar, and immediate recent feed) within the primary first-screen fold.
- **Thumb-Zone Optimization:** Move high-frequency interactive triggers (Quick Add, Pindah Saldo, Category filters, Keypad) into the bottom 40% of the screen.
- **Native Bottom Sheet Drawer:** Implement bottom-sheet modals for transaction entry and transaction details that slide up from the bottom on mobile instead of centered desktop dialogs.
- **Unified Navigation:** Eliminate the redundant mobile hamburger menu; use a clean, native-feeling bottom dock with haptic-ready active states.
- **Preserve Desktop Power:** Maintain the rich, multi-column desktop command center on viewports $\ge 1024\text{px}$ without regression.

**Non-Goals:**
- Converting the web application into a React Native or Flutter native app (the web application remains a Next.js/Tailwind web app running in browser/PWA).
- Removing existing desktop features or data visualizations.

## Decisions

### 1. Adaptive View Composition (`lg:hidden` vs `hidden lg:block`)
- *Decision:* Structure the layout components with clear responsive boundaries:
  - On the Home page (`frontend/src/app/page.tsx`), encapsulate the mobile-first flow in a dedicated `<MobileHomeView />` alongside the existing `<DesktopHomeView />` (or adaptive sub-components).
  - *Rationale:* Trying to force a single JSX DOM tree to be both a dense 3-column desktop cockpit and a thumb-first single-handed mobile app leads to fragile CSS hacks. Separating view composition ensures clean, uncompromised code for both viewports.

### 2. Native Bottom-Sheet for Quick Capture
- *Decision:* Update `Modal.tsx` and `QuickCaptureModal.tsx` to detect mobile viewports and render as an anchored bottom sheet:
  - Slide up animation from `bottom-0`.
  - Rounded top corners (`rounded-t-3xl`).
  - Tactile top drag bar.
  - Number pad and submit button anchored to the bottom.
  - *Rationale:* Desktop modals placed in the center of the screen are awkward to interact with on mobile and often get obscured by the software keyboard.

### 3. Streamlined Activity Feed with Date Grouping
- *Decision:* On mobile, group transactions by date headers (`Hari Ini`, `Kemarin`, `DD MMMM YYYY`), showing clear colored type icons, clean typography with tabular figures, and tapping a row opens a detail bottom sheet.
- *Rationale:* Matches the mental model of modern banking apps (iOS Wallet, Wise, Bank Jago).

### 4. Simplified Mobile Top Bar
- *Decision:* Remove the hamburger drawer button. The mobile top bar becomes a lightweight header displaying user greeting, statement month stepper pill, balance visibility eye, and profile settings icon.
- *Rationale:* All primary navigation destinations are accessible from the bottom bar, eliminating visual clutter.

## Risks / Trade-offs

- *[Risk]* Layout duplication might increase component LOC.
  - *Mitigation:* Both mobile and desktop views consume the exact same React Query hooks (`useQuery`, `useMutation`), data models, and formatting utilities (`fmtMoney`, `bal`). Only the layout presentation differs.
- *[Risk]* Software keyboard covering input fields on mobile.
  - *Mitigation:* Bottom sheet uses `interactive-widget=resizes-content` viewport behavior and virtual keypad by default, avoiding awkward virtual keyboard popups.
