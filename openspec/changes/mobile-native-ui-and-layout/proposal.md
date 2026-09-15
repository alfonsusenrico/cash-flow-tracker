## Why

Currently, the web interface behaves primarily as a desktop-oriented dashboard that collapses into a single, vertically stacked column on mobile viewports. On smartphone screens (360px–430px wide), users must scroll through 12–15 heavy analytical cards and desktop-sized components before reaching recent activity or primary accounts. Furthermore, navigation relies on desktop modals, desktop pagination, and a duplicate hamburger drawer alongside a bottom glass pill.

True mobile personal finance apps (e.g. Wise, Revolut, Apple Card, Bank Jago) follow a completely different information hierarchy and touch-first ergonomic model. Instead of simply shrinking desktop components to fit narrow viewports, the mobile experience must be designed from the ground up as if the project were conceived primarily for mobile devices—featuring thumb-zone action clusters, bottom sheets, streamlined date-grouped feeds, horizontal carousels, and dedicated mobile screens that coexist cleanly with the desktop command center.

## What Changes

- **Dedicated Mobile Layout Architecture:** Separate the layout logic so that viewports under 1024px render dedicated, touch-optimized mobile screens rather than stacked desktop cards, while preserving the multi-column command center on desktop.
- **Mobile Home (Pulse & Feed):** Re-architect the mobile home screen into a thumb-friendly hierarchy:
  - Sticky compact mobile header with greeting, month indicator pill, balance privacy toggle, and profile settings avatar.
  - Card-free or minimal-elevation Hero Balance tile with 1-tap toggle between Liquid Cash and Invested Assets.
  - 3 primary thumb-reach action pills right under the balance: `[ + Catat ]`, `[ ⇄ Pindah ]`, `[ ⚡ Alokasi ]`.
  - Single-row Kakeibo 50/30/20 budget pace indicator bar.
  - Immediate Recent Activity Timeline (promoted above analytical charts for zero-scroll glanceability).
- **Mobile Ledger (Date-Grouped Feed):** Redesign the mobile transactions screen with horizontal category filter chips, search drawer, date-grouped items (`Hari Ini`, `Kemarin`, `12 Sep 2026`), and tap-to-open transaction detail bottom sheets.
- **Mobile Accounts (Vault & Pockets):** Replace bulky vertical account cards with horizontal carousels or dense 2-column card decks, featuring tap-to-expand child pockets (e.g. Bank Jago pockets) and a 2-step transfer bottom sheet.
- **Mobile Bottom-Sheet Quick Capture:** Replace centered desktop modals on mobile with native bottom-sheet drawers (`rounded-t-3xl`), featuring a prominent numerical display, 3x4 thumb keypad, 1-tap fast amount chips (`+10k`, `+50k`, `+100k`), and horizontal category chips.
- **Unified Navigation & Elimination of Duplication:** Retire the redundant mobile hamburger menu in favor of a clean, persistent native bottom navigation bar with 4 primary destinations (Beranda, Transaksi, Analisis, Dompet) plus a center quick-add trigger.

## Capabilities

### New Capabilities
- `mobile-native-layout`: Bespoke, mobile-first UI architecture, thumb-friendly navigation, bottom-sheet transaction drawers, date-grouped feeds, and touch-optimized pocket management designed specifically for handheld mobile devices.

### Modified Capabilities
- `tactile-pulse-ui`: Update the mobile responsive requirements so that handheld viewports deliver a dedicated native mobile experience rather than stacked desktop components.

## Impact

- **Frontend Layout:** `frontend/src/components/layout/AppLayout.tsx`, `TopBar.tsx`, `BottomNav.tsx`, `Sidebar.tsx`.
- **Frontend Pages:** `frontend/src/app/page.tsx` (Home), `frontend/src/app/ledger/page.tsx`, `frontend/src/app/accounts/page.tsx`, `frontend/src/app/insights/page.tsx`.
- **Components & Modals:** `frontend/src/components/ui/QuickCaptureModal.tsx`, `frontend/src/components/ui/Modal.tsx`, `frontend/src/components/ui/DetailDrawer.tsx`.
- **Dependencies:** None required; implemented with pure Tailwind CSS and React/Next.js components.
