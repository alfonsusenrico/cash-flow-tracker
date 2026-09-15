# Proposal: Modern Fintech UI/UX Redesign (Kazz Reference) & Kakeibo Lifestyle Engine

## Why

The Cash Flow Tracker was recently consolidated into a minimal 5-entity core backend, but its frontend presentation remains an early prototype layout with flat summary cards and basic forms. Researching modern acclaimed personal finance trackers—specifically **Kazz** (`dev.kalsel.kazz`), **Ivy Wallet**, and **Copilot Money**—reveals significant opportunities to elevate the user experience:
1. **Visual Atmosphere & Tactility:** Deep charcoal slate dark surfaces (`#0F1115` base, `#171A21` card surface) with subtle 1px structural borders and vibrant semantic accents provide high contrast and zero visual fatigue.
2. **Desktop Cockpit Efficiency:** An asymmetric 2-column workbench (62% primary flow vs 38% auxiliary insight/pockets) avoids empty desktop canvas while delivering high data density.
3. **Mobile Speed & Ergonomics:** A floating bottom capsule dock and a tactile quick capture modal with a custom 4x4 calculator keypad and inline math.
4. **Intelligent Lifestyle Insights (Kakeibo & Narratives):** Incorporating Kakeibo 3-pillar breakdown (*Need* $\le 50\%$, *Want* $\le 30\%$, *Saving* $\ge 20\%$), spend velocity pacing, period-over-period delta indicators ($\Delta\%$), and natural language summaries so users immediately understand their spending habits without cognitive friction.

## What Changes

- **Database:**
  - Create migration `db/migrations/V11__kakeibo_classification.sql` adding `kakeibo_type` (`need`, `want`, `saving`) to `categories` and `transactions`, seamlessly mapped from existing `is_primary` flags.
- **Backend Analytics Engine (`dashboard.py` & `insights.py`):**
  - Compute period-over-period comparison metrics ($T_0$ vs $T_{-1}$) including `prev_inflow`, `prev_outflow`, `net_cashflow_delta_pct`, and category variances.
  - Calculate Kakeibo lifestyle ratios (`need_spent`, `need_pct`, `want_spent`, `want_pct`, `saving_spent`, `saving_pct`) with ideal threshold evaluations.
  - Expose spend velocity and daily allowance drift taking into account upcoming recurring obligations.
  - Generate natural language Indonesian narrative insight summaries identifying primary spend drivers.
- **Frontend Design System (`tokens.css` & `globals.css`):**
  - Implement deep charcoal slate dark theme (`#0F1115` / `#171A21`), refined light theme, and high-energy semantic tokens.
- **Frontend Components (`frontend/src/components/`):**
  - `HeroWalletCard.tsx`: Multi-wallet balance card with masked privacy eye toggle (`••••••`) and horizontal Quick Action Capsules.
  - `DailyBudgetBar.tsx`: Linear budget gauge with moving cursor dot and daily safe-to-spend tracking.
  - `MetricMatrixGrid.tsx`: 2x2 financial pillars grid (Inflow, Outflow, Net, Savings Rate) with dotted sparkline matrix and delta badges.
  - `CategoryDonutChart.tsx`: Center-stat donut chart displaying dominant category percentage & name, alongside clean legend and highlight pill banner.
  - `NarrativeInsightCard.tsx`: Period comparison callout card with `MENURUN` / `MENINGKAT` status badge and natural language summary.
  - `QuickCaptureModal.tsx`: Redesigned quick-entry drawer with Kakeibo tags, large focal amount display, and 4x4 tactile calculator keypad with inline math.
  - `BottomNav.tsx`: Modern floating capsule dock with active pill indicator and quick capture FAB.
  - `Sidebar.tsx`: Sleek persistent desktop navigation rail.
- **Pages (`app/page.tsx` & `app/insights/page.tsx`):**
  - Re-architect Dashboard into 2-column asymmetric desktop cockpit.
  - Add `[Ringkasan]` | `[Bandingkan]` comparison toggle in Insights.

## Capabilities

### New Capabilities
- `kakeibo-lifestyle-tracking`: 3-pillar breakdown of expenditures into Needs, Wants, and Savings with lifestyle inflation alerts.
- `narrative-financial-insights`: Deterministic generation of human-friendly cash flow narrative summaries and period-over-period variance tracking.
- `tactile-calculator-keypad`: 4x4 touch keypad with inline arithmetic expression evaluation (`+ - × ÷ =`) and instant submit.
- `floating-capsule-navigation`: Floating bottom navigation dock for mobile viewports.

### Modified Capabilities
- `dashboard-overview`: Enriched with Kakeibo distribution, velocity index, and period delta comparisons.
- `transaction-entry`: Enriched with optional Kakeibo classification tags and calculator support.

## Impact

- **Affected Files:**
  - `db/migrations/V11__kakeibo_classification.sql`
  - `backend/app/routers/dashboard.py`
  - `backend/app/routers/insights.py`
  - `backend/tests/test_kakeibo_and_insights.py`
  - `frontend/tokens.css`
  - `frontend/src/app/globals.css`
  - `frontend/src/components/dashboard/HeroWalletCard.tsx`
  - `frontend/src/components/dashboard/DailyBudgetBar.tsx`
  - `frontend/src/components/dashboard/MetricMatrixGrid.tsx`
  - `frontend/src/components/dashboard/CategoryDonutChart.tsx`
  - `frontend/src/components/dashboard/NarrativeInsightCard.tsx`
  - `frontend/src/components/ui/QuickCaptureModal.tsx`
  - `frontend/src/components/layout/BottomNav.tsx`
  - `frontend/src/components/layout/Sidebar.tsx`
  - `frontend/src/app/page.tsx`
  - `frontend/src/app/insights/page.tsx`
- **Dependencies:** None added (uses existing Recharts, Tailwind, Lucide/Icon system).
- **Backward Compatibility:** 100% preserved. `is_primary` defaults smoothly map to `need`/`want`.

## Expected Outcome

- **Desktop Users ($\ge$ 1024px):** Enjoy an asymmetric cockpit layout with clear data density, immediate glanceability of daily allowance, 2x2 metric matrix with dotted sparklines, and center-stat donut charts without dead space.
- **Mobile Users (< 1024px):** Experience a fluid mobile app with floating capsule navigation, 1-tap quick capture with tactile 4x4 calculator keypad, and Kakeibo tags.
- **Financial Intelligence:** Users clearly understand whether their spending is dominated by necessities or lifestyle inflation, and understand exactly why their expenses jumped or dropped via conversational summaries.
