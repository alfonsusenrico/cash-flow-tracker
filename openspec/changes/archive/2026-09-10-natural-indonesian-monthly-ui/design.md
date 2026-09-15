## Context

The user experience audit revealed two major frictions:
1. **Confusing dual temporal controls**: Users had both a cycle stepper and a 30d/90d rolling pill in the TopBar, leading to conflicting calculations and user uncertainty about which filter controlled what.
2. **Unnatural or bureaucratic Indonesian terminology**: The UI had remnants of machine translation ("Siklus Ini"), bureaucratic government accounting terms ("Realisasi"), and awkward enterprise labels ("Portofolio Target Tabungan").

## Goals / Non-Goals

**Goals:**
- Unify time navigation around an explicit month-and-year stepper `[ < September 2026 > ]` (or `[ < Sep 2026 > ]`).
- Remove the `[ 30 Hari | 90 Hari ]` pill from the global TopBar and the Insights page.
- Clean up all component labels in frontend views and backend response strings to match modern Indonesian consumer finance standards (Bank Jago, Bibit, BCA).
- Strictly retain accurate, natural terms such as `"Sesuaikan Saldo"` while prohibiting colloquial slang ("Boncos", "Tekor").

**Non-Goals:**
- Altering the database schema or transaction persistence model.
- Removing backend support for 30d/90d query parameters (they remain functional in API endpoints if needed internally, but the UI focuses on the monthly cycle).

## Decisions

### Decision 1: Month and Year Stepper (`getCycleMonthLabel`)
- **Choice**: Compute the active month and year dynamically in `TopBar.tsx` using `toLocaleDateString("id-ID", { month: "short", year: "numeric" })` based on `cycleOffset`.
- **Display**:
  - Offset = 0: `Sep 2026`
  - Offset = -1: `Agu 2026`
  - When offset $\neq 0$: display a small `"Bulan Ini"` action next to the chevrons to return immediately.
- **Alternatives Considered**: Keeping "Bulan Ini" without the year/month name was rejected because showing "Sep 2026" gives immediate temporal grounding.

### Decision 2: Elimination of Dual-Timeframe Pills
- **Choice**: Completely remove the `[ Siklus | 30 Hari | 90 Hari ]` button group from `TopBar.tsx` and `insights/page.tsx`. Default the app context `timeframe` to `"cycle"`.
- **Rationale**: Eliminates visual clutter and avoids cognitive clash between rolling days and monthly statements.

### Decision 3: Precision Indonesian Vocabulary Mapping
- **Choice**:
  - `Realisasi` $\rightarrow$ `Terpakai`
  - `Arus Kas Kumulatif` $\rightarrow$ `Grafik Uang Masuk & Keluar`
  - `Distribusi Pengeluaran` $\rightarrow$ `Pengeluaran per Kategori`
  - `Laju Pengeluaran Harian` $\rightarrow$ `Pengeluaran Harian`
  - `Portofolio Target Tabungan` $\rightarrow$ `Target Tabungan`
  - `Pelunasan Tagihan & Utang` $\rightarrow$ `Pelunasan Cicilan & Utang`
  - `Sesuaikan Saldo` $\rightarrow$ Kept as-is (standard banking term)
  - `Siklus Gajian & Mata Uang` $\rightarrow$ `Tanggal Gajian & Mata Uang`
  - Backend label: `Siklus (25 Jan - 24 Feb)` $\rightarrow$ `Periode 25 Jan - 24 Feb`

## Risks / Trade-offs

- **[Loss of trailing 30d/90d view]** $\rightarrow$ Mitigation: The average 30-day burn rate is still computed by the backend and displayed on the Insights and Net Worth KPI ribbons as contextual statistics.
- **[Payday cycle vs calendar month mismatch]** $\rightarrow$ Mitigation: The stepper shows the target month (e.g. "Sep 2026"), while the subtitle / hero badge explicitly displays the exact date window (e.g. "Periode 25 Agu - 24 Sep"), providing complete transparency.
