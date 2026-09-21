## Why

Currently, the web interface behaves primarily as a desktop dashboard that collapses into a single, vertically stacked column on mobile viewports. On smartphone screens (360px–430px wide), this results in severe ergonomic and visual issues:
- **Home (`/`):** The 4 primary action buttons stacked vertically instead of a horizontal thumb row, balances and widgets took excessive vertical space, pushing the activity feed far below the fold.
- **Ledger (`/ledger`):** Two bulky full-width HTML `<select>` dropdowns ("Semua Rekening", "Semua Kategori") and desktop stats cards consumed over 60% of the screen before the first transaction was visible.
- **Insights (`/insights`):** 3 giant stacked cards for daily averages, and a full 6-column desktop data table (`<table>`) for category budgets squeezed into narrow mobile widths with cut-off columns.
- **Accounts (`/accounts`):** Complex multi-level hierarchy with nested pocket cards, desktop drag-and-drop handles, and buy/sell buttons crammed into narrow cards.

As the user explicitly directed: **"Assume the web-ui doesn't exist and the task is to build the app specifically for mobile device size resolution."** A true mobile fintech application (e.g. Apple Card, Revolut, Copilot Money, Bank Jago) is built from mobile-first ergonomic principles: thumb-reach actions, horizontal scrolling decks, slide-up bottom sheets, progress cards instead of data tables, and zero-scroll primary glances.

## What Changes

- **Pure Mobile View Separation (`lg:hidden` vs `hidden lg:block`):**
  - Dedicated mobile-first screen components for all 4 primary screens (`Home`, `Ledger`, `Insights`, `Accounts`) under 1024px.
  - Complete preservation of the rich multi-column desktop cockpit on viewports $\ge 1024\text{px}$.
- **Screen 1 — Mobile Home (Beranda):**
  - Crisp Hero Balance (no enclosing border box) with 1-tap toggle between Total and Liquid Cash.
  - Guaranteed horizontal 4-action row (`flex justify-between w-full`, 4 tactile 56px squircles: `Catat`, `Transfer`, `Alokasi`, `Laporan`).
  - Horizontal snap-scroll card deck for Accounts (Cash, Bank, E-wallet, Investment) styled like Apple Wallet cards.
  - Compact safe-to-spend allowance pill with 4px 3-color Kakeibo bar (Need 50%, Want 30%, Save 20%).
  - Immediate Recent Activity Feed starting in the primary fold with date grouping and 40px category glyphs.
- **Screen 2 — Mobile Ledger (Transaksi):**
  - 1-row cash flow ribbon (`Masuk: +50rb` | `Keluar: -222rb` | `Net: -172rb`).
  - Search input with horizontal scrolling type chips (`Semua`, `Keluar`, `Masuk`).
  - **Elimination of bulky desktop `<select>` dropdowns:** Replaced with a `[ ⚙️ Filter ]` button that opens a clean Bottom Sheet Filter Drawer for picking Accounts and Categories.
  - Date-grouped mobile transactions feed (`MobileLedgerFeed`) with sticky date headers.
  - Tap row opens slide-up Detail/Edit Bottom Sheet.
- **Screen 3 — Mobile Insights (Analisis):**
  - 1-row 3-metric horizontal KPI strip (Rata 7 Hari, Rata 30 Hari, Proyeksi) in place of 3 stacked vertical boxes.
  - Compact touch-friendly spending cadence chart (160px height).
  - **Elimination of desktop HTML `<table>`:** Category budgets rendered as native **Mobile Progress Cards** with category icon, name, spent vs budget, status pill, 6px colored progress bar, and 1-tap edit trigger.
  - Mobile Kakeibo 3-pillar breakdown and period-over-period comparison cards.
- **Screen 4 — Mobile Accounts (Dompet & Rekening):**
  - Streamlined Net Worth summary card (Total Net Worth, Runway badge, Liquid Cash vs Debt).
  - Account category segmented control (`Semua`, `Bank`, `Kas`, `Investasi`).
  - Mobile account cards with clean expandable pocket accordions (`Kantong (4) ▼`), eliminating desktop drag-and-drop handles and nested table layouts.
- **Modals & Quick Capture:**
  - All modals render as native slide-up Bottom Sheets (`rounded-t-[32px]`) with drag handle and thumb-zone numeric keypad.

## Capabilities

### New Capabilities
- `mobile-native-layout`: Pure mobile-first UI architecture, thumb-friendly navigation, bottom-sheet transaction drawers, date-grouped feeds, filter bottom sheets, and progress cards designed specifically for handheld mobile devices.

### Modified Capabilities
- `tactile-pulse-ui`: Update the mobile responsive requirements so that handheld viewports deliver a dedicated native mobile experience rather than stacked desktop components.

## Impact

- **Frontend Layout:** `frontend/src/components/layout/AppLayout.tsx`, `TopBar.tsx`, `BottomNav.tsx`.
- **Frontend Pages:**
  - `frontend/src/app/page.tsx` & `frontend/src/components/dashboard/MobileHomeView.tsx` (Home).
  - `frontend/src/app/ledger/page.tsx` & `frontend/src/components/ledger/MobileLedgerFeed.tsx` (Ledger).
  - `frontend/src/app/insights/page.tsx` (Insights & Category Budgets).
  - `frontend/src/app/accounts/page.tsx` (Accounts & Pockets).
- **Modals:** `frontend/src/components/ui/Modal.tsx`, `QuickCaptureModal.tsx`.
