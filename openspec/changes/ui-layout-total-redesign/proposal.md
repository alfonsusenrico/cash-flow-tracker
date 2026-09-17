## Why

The current UI implementation adopted the correct design tokens (colors, typography, borders) from
the Scandinavian Tactile Minimalist reference set (`ui ref [1-8].jpg`), but the **page layouts were
not rebuilt to match the reference spatial structure**. The result is that all four screens still
use a card-elevation approach (white boxes floating on a gray canvas with border + shadow) whereas
the references uniformly use a **flat, edge-to-edge workbench with 1px structural dividers** between
sections — the visual signature of Linear / Elera / the reference set.

Specific gaps:
1. **Inner sections are elevated cards** — `card-crisp` adds border, background, shadow per section.
   The refs show flat sections directly inside the window with only 1px dividers.
2. **Page-level headings inside the workbench** — The refs have no large page `<h1>` inside the
   content area; the TopBar provides the page context. Content starts directly with data.
3. **KPI stat cards** — Current cards have individual white backgrounds; refs show a single unified
   banner row of 4 stats with dividers between them, no outer borders.
4. **Right action rail** in Beranda — Missing the "Do these first / Lakukan Segera" priority queue
   section that is the defining element of the right rail in Refs 1, 4, 8.
5. **Spacing density** — Current spacing uses `space-y-5` with generous padding; refs pack content
   tightly and use section dividers instead of whitespace to separate zones.

## What Changes

**All frontend page layouts and key components are rebuilt** to match the flat-workbench, dense
data-table, and right-action-rail layout from the references. No backend changes.

- **Global:** Remove card elevation from inner workbench sections. Introduce `section-flat`
  utility class (flat background, 1px border-bottom divider, zero box shadow).
- **Beranda (`/`):** Rebuild to flat KPI stat row → flat 2-column workbench: left = kakeibo
  allocation + dense feed table; right = "Lakukan Segera" action queue + mini vault.
- **Ledger (`/ledger`):** Rebuild header to compact filter-only strip (no page h1). Table occupies
  full height. Summary bar becomes a slim inline strip above the table header row.
- **Insights (`/insights`):** Rebuild to match Ref 7 — bar chart + bullet benchmark cards in a
  2-column grid (chart left, bullet cards right scrollable column).
- **Accounts (`/accounts`):** Rebuild net worth header to match Ref 2 — single flat strip with
  4 liquidity metrics; account cards become flat list rows, not elevated individual cards.
- **KPI StatCard component:** Refactor to single-row strip with `border-r` dividers, not
  individual cards.

## Capabilities

### New Capabilities
- `flat-workbench-layout`: Flat, zero-elevation workbench inside the app window — sections use
  1px border-bottom dividers instead of card elevation. All 4 screens adopt this.
- `lakukan-segera-rail`: Right action rail in Beranda with "Lakukan Segera" (priority tasks
  from pending scheduled bills + unconfirmed mobile notifications) matching Refs 1, 4, 8.
- `kpi-stat-row`: Unified 4-stat horizontal row with `border-r` column dividers, replacing
  individual `StatCard` elevated boxes on the Beranda and Insights screens.

### Modified Capabilities
- `beranda-dashboard`: Layout rebuilt — flat sections, KPI row, kakeibo track, feed table,
  right action rail with "Lakukan Segera" queue.
- `ledger-table`: Compact filter strip replaces page header section; table full-height.
- `insights-analytics`: 2-column layout — daily cadence bar chart left, bullet benchmark
  cards right scrollable; matches Ref 7 density.
- `accounts-vault`: Net worth banner becomes flat 4-metric strip; account rows are flat
  list rows instead of individual raised cards.

## Impact

- **Modified files:**
  - `frontend/src/app/globals.css` — Add `section-flat`, `kpi-row`, `kpi-cell` utility classes.
  - `frontend/src/components/ui/StatCard.tsx` — Refactor to KPI row cell variant.
  - `frontend/src/app/page.tsx` — Full layout rebuild.
  - `frontend/src/app/ledger/page.tsx` — Layout rebuild (filter strip + table).
  - `frontend/src/app/insights/page.tsx` — Layout rebuild (2-column).
  - `frontend/src/app/accounts/page.tsx` — Net worth strip + flat account rows.
- **No backend changes.**
- **No database changes.**
- **No API changes.**
- **No new dependencies.**

## Expected Outcome

All four screens adopt the flat-workbench spatial structure from the reference UI images:
- No elevated white card boxes inside the content area.
- KPI metrics rendered as a single horizontal stat row with dividers.
- "Lakukan Segera" right action queue appears on Beranda.
- Ledger opens directly on the dense table with no tall page heading section.
- Insights: bar chart left, benchmark cards right in a 2-column scrollable workbench.
- Accounts: flat net worth strip + list-style account rows matching Ref 2.
