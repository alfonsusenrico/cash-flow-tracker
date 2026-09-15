# Tasks: Modern Fintech UI/UX Redesign & Kakeibo Lifestyle Engine

## 1. Database Migration & Schema

- [x] 1.1 Create migration `db/migrations/V11__kakeibo_classification.sql` adding `kakeibo_type` (`need`, `want`, `saving`) to `categories` and `transactions` tables.
- [x] 1.2 Apply migration to `ledger_db` and verify column definitions and default backfill.

## 2. Backend Analytics & Intelligence Engine

- [x] 2.1 Update `backend/app/routers/dashboard.py` to calculate previous cycle metrics ($T_{-1}$) and percentage deltas (`inflow_delta_pct`, `outflow_delta_pct`, `net_cashflow_delta_pct`, `savings_rate_delta_pts`).
- [x] 2.2 Implement Kakeibo ratio calculation (`need_spent`, `need_pct`, `want_spent`, `want_pct`, `saving_spent`, `saving_pct`, `kakeibo_status`) in `dashboard.py` and `insights.py`.
- [x] 2.3 Implement natural language Indonesian narrative insight generator identifying primary spend drivers.
- [x] 2.4 Incorporate spend velocity index and upcoming planned recurring deductions into daily allowance calculation.
- [x] 2.5 Write automated test suite `backend/tests/test_kakeibo_and_insights.py` and ensure 100% pytest pass rate.

## 3. Design Tokens & Styling Upgrade

- [x] 3.1 Update `frontend/tokens.css` with Kazz-inspired deep charcoal slate surfaces, 1px hairline borders, and high-energy semantic colors.
- [x] 3.2 Update `frontend/src/app/globals.css` with refined card elevations, pill borders, and dark glassmorphism effects.

## 4. Frontend Component Construction

- [x] 4.1 Create `frontend/src/components/dashboard/HeroWalletCard.tsx` featuring multi-wallet balance, privacy eye masking, and horizontal Quick Action Capsules.
- [x] 4.2 Create `frontend/src/components/dashboard/DailyBudgetBar.tsx` featuring visual linear track with moving cursor dot and daily safe-to-spend tracking.
- [x] 4.3 Create `frontend/src/components/dashboard/MetricMatrixGrid.tsx` featuring 2x2 financial pillars with dotted matrix sparklines and delta badges.
- [x] 4.4 Update `frontend/src/components/dashboard/CategoryDonutChart.tsx` with dynamic center stat (dominant % and category name), clean legend table, and highlight pill banner.
- [x] 4.5 Create `frontend/src/components/dashboard/NarrativeInsightCard.tsx` featuring period comparison trend callout and conversational summary.
- [x] 4.6 Redesign `frontend/src/components/ui/QuickCaptureModal.tsx` with Kakeibo 3-way chips (`Need`, `Want`, `Saving`), date/account pills, large amount display, and 4x4 tactile calculator keypad with live math.
- [x] 4.7 Update `frontend/src/components/layout/BottomNav.tsx` to a detached floating capsule dock with active pill highlight.
- [x] 4.8 Refine `frontend/src/components/layout/Sidebar.tsx` navigation styling.

## 5. Page Assembly & Responsiveness

- [x] 5.1 Rebuild `frontend/src/app/page.tsx` (Dashboard) into the 2-column asymmetric desktop cockpit layout (62% primary / 38% auxiliary) and stacked mobile flow.
- [x] 5.2 Update `frontend/src/app/insights/page.tsx` with `[Ringkasan]` | `[Bandingkan]` segmented switch and Kakeibo breakdown tab.

## 6. End-to-End Verification & Validation

- [x] 6.1 Run frontend type-check, lint, and production build (`npm run type-check && npm run lint && npm run build`).
- [x] 6.2 Rebuild and restart Docker containers (`ledger_api` and `ledger_frontend`).
- [x] 6.3 Validate OpenSpec change with `openspec validate modern-fintech-ui-and-kakeibo-redesign`.
- [x] 6.4 Perform end-to-end smoke testing across desktop and mobile viewports.
