## Context

The cash flow tracker previously attempted responsive styling by shrinking desktop components and stacking them vertically into a single column. As evidenced by production testing on simulated mobile viewports (e.g. 430px iPhone 14 Pro Max):
- Action buttons collapsed or stacked vertically due to invalid or fragile classes.
- Desktop search/filter panels with two full-width native `<select>` dropdowns filled the entire mobile screen above the transaction list.
- An HTML `<table>` with 6 columns was squeezed into narrow mobile widths on the Insights page, causing text clipping and overflow.
- Accounts displayed nested tables, desktop drag handles, and crowded buy/sell buttons.

User directive: **"Assume the web-ui doesn't exist and the task is to build the app specifically for mobile device size resolution."**

## Goals / Non-Goals

**Goals:**
- **Zero-Scroll Primary Pulse:** On mobile, place the core status (Available balance, Today's safe allowance, Kakeibo pace bar, and immediate recent feed) within the primary first-screen fold.
- **Thumb-Zone Quick Actions:** Horizontal 4-action touch bar with 56px touch targets (`Catat`, `Transfer`, `Alokasi`, `Laporan`).
- **Eliminate Desktop Form Clutter on Mobile:** Replace stacked desktop `<select>` dropdowns with search pills + horizontal filter chips + bottom sheet filter drawers.
- **Eliminate Desktop Tables on Mobile:** Replace HTML `<table>` for category budgets with native mobile **Progress List Cards**.
- **Apple Wallet Style Account Cards:** Replace desktop account tables and nested panels with clean mobile cards and expandable accordion pockets.
- **Native Bottom Sheet Drawer:** Implement slide-up bottom sheets (`rounded-t-[32px]`) for quick capture, filter selection, and transaction detail editing.
- **Preserve Desktop Power:** Maintain the rich, multi-column desktop command center on viewports $\ge 1024\text{px}$ without regression.

**Non-Goals:**
- Removing any data visualization or analysis capabilities from the desktop command center.
- Introducing heavy 3rd-party mobile UI libraries (we use Tailwind CSS, standard React primitives, and existing Lucide icons).

## Decisions & Screen Architectures

### 1. View Separation Boundary (`lg:hidden` vs `hidden lg:block`)
- Handheld viewports (`< 1024px`) render bespoke mobile components.
- Desktop viewports (`>= 1024px`) render the full command-center cockpit.
- Shared data models and React Query hooks ensure zero business logic duplication.

### 2. Screen 1 — Mobile Home (`MobileHomeView`)
- **Hero Balance:** Clean tabular text (`Rp 30.381.227`) with pill toggle between Total and Liquid Cash.
- **4 Action Buttons:** Strict flex row: `flex items-center justify-between w-full px-2 py-2` using `flex-1` per button. 56px squircle containers with icons and bold 11px labels: `Catat`, `Transfer`, `Alokasi`, `Laporan`.
- **Rekening Carousel:** Horizontal snap-scroll deck of tactile cards (`w-[200px] h-[105px]`) for quick balance glances.
- **Sisa Aman & Kakeibo Pill:** Compact card with daily allowance (`Rp 0 / hari`), velocity badge (`● Terkendali`), and 4px 3-color segmented bar (Need 50%, Want 30%, Save 20%).
- **Recent Activity Feed:** Direct chronological list with 40px rounded category glyphs, payee names, accounts, timestamps, and tabular amounts.

### 3. Screen 2 — Mobile Ledger (`MobileLedgerFeed`)
- **1-Row Cash Flow Strip:** Compact bar displaying `Masuk (+Rp 50k) | Keluar (-Rp 222k) | Net (-Rp 172k)`.
- **Search & Filter Drawer:**
  - Sleek search input pill.
  - Horizontal scrolling type chips (`Semua`, `Keluar`, `Masuk`).
  - **`[ ⚙️ Filter ]` button:** Opens a **Bottom Sheet Filter Drawer** for Account and Category selection. Eliminates the 2 bulky native `<select>` dropdowns from the main screen.
- **Date-Grouped Feed:** Sticky date headers (`15 September 2026`, total sum), tactile transaction cards, and tap-to-open bottom sheet for editing.

### 4. Screen 3 — Mobile Insights (`/insights`)
- **1-Row KPI Strip:** 3-column grid (`grid grid-cols-3 gap-2`) with 7-day average, 30-day average, and cycle projection cards (height ~75px) replacing giant stacked boxes.
- **Touch Spending Rhythm Chart:** Compact Recharts bar chart (160px height) with peak day highlighted in Spring Lime (`#66CC55`).
- **Category Budgets (Mobile Progress Cards):** Replaces the desktop 6-column HTML `<table>`. Each category is rendered as a clean card:
  - Top: Icon + Category Name | Terpakai: `Rp 201.600`
  - Middle: Status badge (`Aman` / `Waspada` / `Lewat`) | Batas: `Rp 500.000` (Sisa: `Rp 298.400`)
  - Bottom: 6px smooth progress bar.
  - Action: Tap card or press `Ubah Batas` to open the category edit modal.

### 5. Screen 4 — Mobile Accounts (`/accounts`)
- **Net Worth Summary:** Clean card with Total Net Worth (`Rp 26.727.210`), Runway badge (`26.5x`), and asset breakdown bar.
- **Segmented Control:** `[ Semua ]` `[ Bank ]` `[ Tunai ]` `[ Investasi ]`.
- **Apple Wallet Style Account Cards:** Clean tactile cards with bank glyphs, account balance, and expandable accordion pockets (`Kantong (4) ▼`) without desktop drag handles or overflowing forms.

### 6. Quick Capture & Detail Bottom Sheets
- All modal dialogs on mobile slide up from the bottom with `rounded-t-[32px]`, drag handles, and thumb-friendly numeric keypads.

## Risks & Mitigations

- *[Risk]* Layout divergence between desktop and mobile.
  - *Mitigation:* Both views consume identical queries (`api.get('/dashboard/overview')`, `api.get('/transactions')`, `api.get('/accounts')`). All mutations invalidate identical query keys.
- *[Risk]* Recharts rendering bugs on mobile Safari.
  - *Mitigation:* Use `ResponsiveContainer` with fixed 160px height and standard SVG primitives.
