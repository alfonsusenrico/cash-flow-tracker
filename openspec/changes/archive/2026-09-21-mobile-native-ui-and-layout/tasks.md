# Tasks: Mobile-Native UI & Handheld Resolution Layout

## Phase 1: Native Mobile Home View (`MobileHomeView.tsx`)
- [x] 1.1 Fix the 4-action row in `MobileHomeView.tsx` with a guaranteed horizontal flex container (`flex flex-row justify-between w-full px-2 py-2`), replacing invalid Tailwind classes with valid `w-14 h-14 rounded-2xl` 56px touch targets for `Catat`, `Transfer`, `Alokasi`, and `Laporan`.
- [x] 1.2 Refactor Hero balance into a clean, card-free app canvas presentation with seamless `Total Kekayaan` vs `Kas Likuid` toggle pill and monthly inflow/outflow capsule.
- [x] 1.3 Implement the Apple Wallet style horizontal snap-scroll account deck (`w-[200px] h-[105px]`) for instant account balance inspection.
- [x] 1.4 Compact the Safe-to-Spend widget and 3-color Kakeibo bar into a single tactile status strip (<95px height).
- [x] 1.5 Promote the chronological Recent Activity feed to begin immediately in the primary viewport with 40px rounded squircle glyphs and tap-to-inspect drawers.

**Expected Result:** Mobile home view presents balance, actions, accounts, safe daily allowance, and recent feed within the first 1.5 screens with zero vertical action stacking bugs.
**Verification Evidence:** Visual verification on 390px/430px mobile viewport confirms 4 horizontal action buttons side-by-side and clean account cards.

## Phase 2: Mobile-Native Ledger Experience (`frontend/src/app/ledger/page.tsx`)
- [x] 2.1 Replace the 2 bulky desktop native `<select>` dropdowns ("Semua Rekening", "Semua Kategori") on mobile with a compact search input pill and horizontal type chips (`Semua`, `Keluar`, `Masuk`).
- [x] 2.2 Add a `[ ⚙️ Filter ]` action button on mobile that opens a dedicated slide-up Bottom Sheet Filter Drawer with touch-friendly account and category selection pills.
- [x] 2.3 Retain the compact 1-row cash flow ribbon (`Masuk: +50rb` | `Keluar: -222rb` | `Net: -172rb`) and date-grouped transaction feed (`MobileLedgerFeed.tsx`).
- [x] 2.4 Ensure desktop view (`hidden lg:block`) retains the full desktop filter grid and 8-column data table without regression.

**Expected Result:** Mobile ledger screen devotes 85% of screen height to transactions instead of being blocked by stacked dropdowns.
**Verification Evidence:** Tapping filter button opens bottom sheet; main screen shows clean search pill, chips, and date-grouped feed.

## Phase 3: Mobile-Native Insights Experience (`frontend/src/app/insights/page.tsx`)
- [x] 3.1 Replace the 3 giant stacked vertical cards on mobile with a compact 1-row 3-metric KPI strip (`grid grid-cols-3 gap-2`, ~75px height) for 7-day average, 30-day average, and cycle projection.
- [x] 3.2 Optimize the daily burn cadence Recharts bar chart for mobile (fixed 160px height) with peak day highlighted in Spring Lime (`#66CC55`).
- [x] 3.3 **Eliminate the 6-column desktop HTML `<table>` on mobile:** Implement native **Mobile Progress Cards** for category budgets with category icon, spent vs budget, status badge, 6px smooth progress bar, and 1-tap edit trigger.
- [x] 3.4 Ensure desktop view (`hidden lg:block`) retains the full 6-column data table and large charts without regression.

**Expected Result:** Category budgets are displayed as mobile progress cards with zero horizontal scrolling or cut-off text; KPI metrics sit in a single compact row.
**Verification Evidence:** Mobile view displays clean progress cards with colored bars and 1-row metrics.

## Phase 4: Mobile-Native Accounts Experience (`frontend/src/app/accounts/page.tsx`)
- [x] 4.1 Streamline the top Net Worth card on mobile into a clean summary tile showing Net Worth, runway badge, and liquid cash vs debt.
- [x] 4.2 Add account type segmented control (`Semua`, `Bank`, `Kas`, `Investasi`) for quick category filtering.
- [x] 4.3 Replace desktop cards with native Apple Wallet style account cards featuring clean expandable pocket accordions (`Kantong (4) ▼`) without desktop drag handles or overflowing forms.
- [x] 4.4 Ensure desktop view (`hidden lg:block`) preserves the multi-column cockpit, drag-and-drop ordering, and desktop modal triggers.

**Expected Result:** Account balances and child pockets are easily browsable with touch-friendly cards and smooth accordions.
**Verification Evidence:** Account cards expand child pockets cleanly on mobile without layout overflow.

## Phase 5: Verification & Production Docker Build
- [x] 5.1 Run frontend type checks (`npm run type-check`) and linting (`npm run lint`) to guarantee type safety and code cleanliness.
- [x] 5.2 Build and restart the local Docker container (`docker compose build frontend && docker compose up -d --no-deps frontend`).
- [x] 5.3 Validate OpenSpec change integrity with `openspec validate mobile-native-ui-and-layout`.

**Expected Result:** Zero build or type errors; Docker container serves the new mobile-native interface on `http://localhost:8090`.
**Verification Evidence:** Terminal output confirms clean build, successful container launch, and passing `openspec validate`.
