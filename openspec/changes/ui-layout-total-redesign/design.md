## Strategy

**Bottom-up, component-by-component.** Start with the shared utility CSS classes and the
`StatCard` component refactor, then rebuild each page in order: Beranda → Ledger → Insights →
Accounts. Each page rebuild is self-contained with no cross-page dependencies.

The guiding principle throughout: **match the spatial structure of the reference images, not just
the colors.** The references share a common spatial grammar:
1. The **window is the container** — no inner padding creates a nested viewport feeling.
2. **Sections are flat** — top-level zones use `border-b` dividers, not elevated card boxes.
3. **The left workbench fills vertically** — content takes up height, not whitespace.
4. **The right rail is 300-340px** and stacks contextual priority cards.
5. **KPI metrics are a single strip** across the full width with `border-r` column separators.

## Data Structures

No new data structures. All data is already available from existing API endpoints:
- `/api/dashboard/overview` → kpis, recent_transactions, accounts_liquidity, goals_glance,
  narrative, kakeibo
- `/api/dashboard/analytics` → burn_rate, daily_cadence, category_variance, kakeibo, narrative
- `/api/accounts` → accounts list
- `/api/transactions` → paginated ledger
- `/api/goals/pending-scheduled` → pending scheduled bills for "Lakukan Segera" queue

The "Lakukan Segera" rail uses the existing `GET /api/goals/scheduled` or the pending-scheduled
banner endpoint that already exists in `PendingScheduledBanner`.

## Component Architecture

### New CSS Utilities (`globals.css`)

```css
/* Flat workbench section — no elevation, 1px bottom border only */
.section-flat {
  background: transparent;
  border-bottom: 1px solid var(--border-divider);
  padding: 16px 24px;
}
.section-flat:last-child { border-bottom: none; }

/* KPI stat row strip */
.kpi-strip {
  display: grid;
  border-bottom: 1px solid var(--border-structural);
}
.kpi-cell {
  padding: 20px 24px;
  border-right: 1px solid var(--border-structural);
  position: relative;
}
.kpi-cell:last-child { border-right: none; }

/* Workbench 2-column split */
.workbench-split {
  display: flex;
  flex: 1;
  min-height: 0;
  overflow: hidden;
}
.workbench-left {
  flex: 1;
  min-width: 0;
  border-right: 1px solid var(--border-structural);
  overflow-y: auto;
}
.workbench-right {
  width: 320px;
  flex-shrink: 0;
  overflow-y: auto;
}
```

### Modified `StatCard.tsx`
Refactored to support a **KPI cell** mode used inside the `kpi-strip` container.
The current grid layout is kept for backwards-compat on smaller screens (mobile cards grid).
A `variant="cell"` prop activates the strip-cell rendering.

### Page Architecture Changes

**Beranda (`page.tsx`)**:
```
<div class="flex flex-col h-full">
  <!-- Pending Scheduled Banner (conditionally shown) -->
  <!-- KPI Strip: 4 cells side by side -->
  <div class="kpi-strip grid-cols-4">
    <div class="kpi-cell">Total Likuiditas</div>
    <div class="kpi-cell">Batas Belanja Hari Ini</div>
    <div class="kpi-cell">Pengeluaran Siklus</div>
    <div class="kpi-cell">Arus Kas Bersih</div>
  </div>
  <!-- Takeaway Banner (full-width) -->
  <TakeawayBanner />
  <!-- 2-column workbench split -->
  <div class="workbench-split">
    <div class="workbench-left flex flex-col divide-y">
      <!-- Kakeibo Allocation Track (section-flat) -->
      <!-- Dense Activity Table (section-flat) -->
    </div>
    <div class="workbench-right flex flex-col divide-y">
      <!-- Lakukan Segera Queue (section-flat) -->
      <!-- Mini Vault (section-flat) -->
      <!-- Mini Goals (section-flat) -->
    </div>
  </div>
</div>
```

**Ledger (`ledger/page.tsx`)**:
```
<div class="flex flex-col h-full">
  <!-- Slim filter strip (no page title) -->
  <div class="section-flat flex gap-2">
    [Type pills] [Account select] [Category select] [Search pill]
  </div>
  <!-- Cash flow summary strip (inline, compact) -->
  <div class="kpi-strip grid-cols-3 text-sm">
    Masuk | Keluar | Bersih
  </div>
  <!-- Dense data table (full-height) -->
  <table class="flex-1 w-full divide-y" />
  <!-- Pagination footer -->
</div>
```

**Insights (`insights/page.tsx`)**:
```
<div class="flex flex-col h-full">
  <!-- Tab switch + actions -->
  <div class="section-flat flex justify-between">
    [Tab pills] [+ Kategori button]
  </div>
  <!-- 2-column: chart (60%) | benchmark cards (40%) -->
  <div class="workbench-split">
    <div class="workbench-left p-6">
      Daily cadence bar chart
      3 Kakeibo pillar cards
    </div>
    <div class="workbench-right divide-y overflow-y-auto">
      Category benchmark cards (scrollable)
      Takeaway banner at bottom
    </div>
  </div>
</div>
```

**Accounts (`accounts/page.tsx`)**:
```
<div class="flex flex-col h-full">
  <!-- Net worth strip (4 KPI cells) -->
  <div class="kpi-strip grid-cols-4">
    Net Worth | Kas Likuid | Investasi | Ketahanan Dana
  </div>
  <!-- Account rows as flat list (not cards) -->
  <div class="flex-1 overflow-y-auto divide-y">
    {accounts.map(acc => <AccountRow />)}
  </div>
</div>
```

## Endpoint Contracts

No changes to backend APIs. All endpoints remain backward-compatible.

## Error Handling

- If `dashboard.kpis` is null (loading/error), show skeleton shimmer in the KPI strip.
- If `pending_scheduled` returns empty, hide the "Lakukan Segera" section entirely (don't show
  an empty card).
- If `accounts_liquidity` is empty, show a "Tambah Rekening" CTA in the mini vault slot.

## Trade-offs & Guardrails

| Decision | Rationale |
|---|---|
| Remove `card-crisp` from inner workbench sections | Matches reference flat aesthetic; card-crisp still valid for modals and overlay panels |
| Keep `card-crisp` on right-rail items | Right rail cards in refs use subtle border + white bg — different from main workbench sections |
| `workbench-split` is CSS flex, not a grid | Flex allows right rail fixed width; responsive collapse at <1024px to stacked layout |
| Page titles removed from workbench | TopBar already provides page context; removing reduces visual noise per refs |
| `kpi-strip` uses `grid-cols-4` | Grid provides even distribution; collapse to `grid-cols-2` on tablet, `grid-cols-1` on mobile |

No background schedulers, no new abstraction layers, no new API endpoints introduced.
