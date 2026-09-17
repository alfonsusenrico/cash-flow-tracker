# Implementation Tasks: ui-layout-total-redesign

## Phase 1: Global CSS Utilities
**Expected Result:** New flat-workbench utility classes available in globals.css.

- [ ] 1.1 Add `kpi-strip`, `kpi-cell`, `section-flat`, `workbench-split`, `workbench-left`,
       `workbench-right` utility classes to `frontend/src/app/globals.css`
- [ ] 1.2 Add `rail-section` class for right-rail flat sections (subtle bg + border-b)

## Phase 2: Beranda (`/`) — Full Layout Rebuild
**Expected Result:** Beranda renders as flat KPI strip → takeaway banner → 2-column split
(kakeibo + feed left | lakukan segera + vault + goals right). No elevated inner cards.

- [ ] 2.1 Replace 4-card grid with flat `kpi-strip` 4-cell row at top of page
- [ ] 2.2 Move TakeawayBanner to full-width slot between KPI strip and workbench split
- [ ] 2.3 Restructure left workbench column: flat kakeibo section + flat activity feed table
       (remove `card-crisp`, add `section-flat`/`divide-y` structure)
- [ ] 2.4 Rebuild right rail with "Lakukan Segera" priority queue using existing
       pending-scheduled data query (hook into PendingScheduledBanner data source)
- [ ] 2.5 Right rail: Mini Vault flat section (accounts liquidity, remove card per account,
       use flat list rows with border-b)
- [ ] 2.6 Right rail: Mini Goals flat section (goal progress rows)
- [ ] 2.7 Remove page-level `<h1>` greeting and subtitle from the workbench content area
       (greeting text moves to TopBar subtitle slot or removed entirely)
- [ ] 2.8 Remove transfer modal inline form — use existing InternalMovementModal instead
       (simplify right rail; connect "Pindah Saldo" button to `openMovement()`)

## Phase 3: Ledger (`/ledger`) — Layout Rebuild
**Expected Result:** Ledger opens directly on compact filter strip + full-height dense table.
No page h1. Cash flow summary rendered inline above table headers.

- [ ] 3.1 Remove page-level `<h1>` heading section from Ledger
- [ ] 3.2 Convert filter area from `card-crisp p-4` to a flat `section-flat` strip with
       `border-b` divider
- [ ] 3.3 Move cash flow summary (Masuk / Keluar / Selisih) from 3-card grid to a slim
       inline strip directly above the table header (using the same `kpi-strip`/`kpi-cell`
       style with 3 cells, smaller font)
- [ ] 3.4 Ensure the data table fills remaining height (use `flex-1` within a `flex flex-col`)

## Phase 4: Insights (`/insights`) — Layout Rebuild
**Expected Result:** Insights uses 2-column flat workbench: daily cadence chart (left) +
bullet benchmark cards (right, scrollable). Takeaway at bottom of right column.

- [ ] 4.1 Remove page-level `<h1>` heading from Insights
- [ ] 4.2 Move tab switch + action buttons to a compact `section-flat` strip at top
- [ ] 4.3 Rebuild to `workbench-split`: left column = 3 Kakeibo pillar cards + bar chart;
       right column = scrollable category benchmark bullet cards
- [ ] 4.4 Takeaway banner moves to bottom of right column as a pinned flat section
- [ ] 4.5 Remove 3-StatCard grid at top (burn rate stats) — merge into tab strip subtitles
       or remove entirely to reduce vertical noise

## Phase 5: Accounts (`/accounts`) — Net Worth + Flat List
**Expected Result:** Accounts shows flat 4-cell net worth strip + flat list account rows.

- [ ] 5.1 Rebuild net worth header from the current multi-card grid to a flat `kpi-strip`
       with 4 cells: Net Worth, Kas Likuid, Total Investasi, Ketahanan Dana
- [ ] 5.2 Convert individual account raised cards to flat list rows with `divide-y`,
       retaining the expand/pocket tree behavior
- [ ] 5.3 Keep `+ Tambah Rekening` charcoal CTA button in a sticky footer strip

## Phase 6: Build Verification
**Expected Result:** All checks pass, Docker container rebuilt.

- [ ] 6.1 Run `npm run type-check` — zero errors
- [ ] 6.2 Run `npm run build` — zero errors
- [ ] 6.3 Run `docker compose up -d --build frontend` — containers healthy
- [ ] 6.4 Manual smoke test: all 4 screens load, KPI strip visible, flat sections verified
