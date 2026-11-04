## REMOVED Requirements

### Requirement: Beranda insight uses the fair comparison
**Reason**: Beranda no longer shows an insight banner. The owner rejected same-date comparisons ("I don't think comparing date to date is good because monthly money management is very volatile", 2026-11-04) and text-heavy cards on Beranda.
**Migration**: Period comparisons are read on Analisis and Laporan with whole months (`compare-whole-months`). Beranda shows only the open period's figures.

### Requirement: Beranda spending so far
**Reason**: The Beranda insight card this rule described is removed.
**Migration**: Beranda's "Pengeluaran" KPI shows the period's spending so far from the cycle summary's `spending`.
