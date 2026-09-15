## Why

The interface previously used an awkward mix of literal English-to-Indonesian translations, machine jargon like "Siklus Ini", bureaucratic accounting terms like "Realisasi", and a redundant dual-timeframe control in the TopBar (both a cycle stepper and a 30d/90d pill). Regular consumers track their money by discrete months ("Berapa belanja saya bulan September?"), not rolling multi-month intervals. Having both controls creates conflicting calculations and high cognitive load. Removing the 30d/90d pill and unifying time navigation around a clean month/year stepper `[ < September 2026 > ]` aligns the app with proven modern banking interfaces like Bank Jago, Bibit, and BCA.

## What Changes

- **Monthly-Only Navigation Stepper**:
  - Remove the `[ 30 Hari | 90 Hari ]` pill filter from `TopBar.tsx` and `insights/page.tsx`.
  - Replace the robotic `"Siklus Ini"` stepper with an explicit month and year stepper: `[ < September 2026 > ]` (or `[ < Sep 2026 > ]` on compact viewports).
  - When shifted back to prior months, show a direct reset action: `"Bulan Ini"`.
- **Refined Natural Indonesian Financial Copy (Bibit / Bank Jago Standard)**:
  - Eliminate all machine jargon: `"Siklus"` → `"Bulan Ini"` / `"Periode <Tanggal> - <Tanggal>"`.
  - Eliminate bureaucratic accounting jargon: `"Realisasi"` → `"Terpakai"`, `"Portofolio Target Tabungan"` → `"Target Tabungan"`, `"Laju Pengeluaran"` → `"Pengeluaran Harian"`.
  - Preserve standard, professional, yet natural terms: **`"Sesuaikan Saldo"`** is strictly retained (zero colloquialisms like "Boncos" or "Tekor").
  - Update `SettingsModal.tsx`: `"Siklus Gajian & Mata Uang"` → `"Tanggal Gajian & Mata Uang"`.
  - Update `backend/app/routers/dashboard.py`: format timeframe labels as `"Periode <start> - <end>"` instead of `"Siklus (<start> - <end>)"`.

## Capabilities

### New Capabilities
- `monthly-navigation-stepper`: Unified month/year temporal stepper (`[ < September 2026 > ]`) with instantaneous month shifting, single-mode cognitive clarity, and quick return to the current active month.

### Modified Capabilities
- `indonesian-daily-finance-ui`: Refine daily Indonesian consumer terminology across all 5 primary views, removing bureaucratic terms, eliminating slang, and preserving authentic banking terms like `Sesuaikan Saldo`.

## Impact

- Affected frontend components:
  - `frontend/src/components/layout/TopBar.tsx`
  - `frontend/src/components/ui/SettingsModal.tsx`
  - `frontend/src/app/page.tsx`
  - `frontend/src/app/insights/page.tsx`
  - `frontend/src/app/accounts/page.tsx`
  - `frontend/src/app/goals/page.tsx`
- Affected backend components:
  - `backend/app/routers/dashboard.py`
- No database migrations or schema alterations required.
