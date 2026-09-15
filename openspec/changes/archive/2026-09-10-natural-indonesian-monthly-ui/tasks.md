## 1. TopBar & Global Time Navigation

- [x] 1.1 Remove dual timeframe pill switcher (`[ 30 Hari | 90 Hari ]`) from `TopBar.tsx` and `insights/page.tsx`
- [x] 1.2 Implement dynamic Month & Year label formatting in `TopBar.tsx` (`[ < Sep 2026 > ]`) with Indonesian locale
- [x] 1.3 Add `"Bulan Ini"` reset action when viewing past months and update chevron tooltips in `TopBar.tsx`

## 2. Backend Timeframe & Settings Modal

- [x] 2.1 Update `backend/app/routers/dashboard.py` timeframe label to `"Periode <start> - <end>"`
- [x] 2.2 Update `SettingsModal.tsx` section heading from `"Siklus Gajian & Mata Uang"` to `"Tanggal Gajian & Mata Uang"`

## 3. Indonesian Daily Financial Copy Refinement

- [x] 3.1 Update `page.tsx` hero cockpit, bento card headings, and chart titles (`"Grafik Uang Masuk & Keluar"`, `"Pengeluaran per Kategori"`, `"Tagihan & Cicilan"`)
- [x] 3.2 Update `BurnCadenceChart.tsx` headers and reference line (`"Pengeluaran Harian"`, `"Batas Belanja: Rp.../hari"`)
- [x] 3.3 Update `insights/page.tsx` table column from `"Realisasi"` to `"Terpakai"` and update section headers
- [x] 3.4 Update `accounts/page.tsx` net worth description and verify `"Sesuaikan Saldo"` remains untouched
- [x] 3.5 Update `goals/page.tsx` headers from `"Portofolio Target Tabungan"` to `"Target Tabungan"` and `"Pelunasan Cicilan & Utang"`

## 4. Verification & Validation

- [x] 4.1 Run frontend type checks (`npm run type-check`) and Next.js production build (`npm run build`)
- [x] 4.2 Rebuild Docker frontend container and smoke test on `http://localhost:8090`
- [x] 4.3 Validate OpenSpec change with `openspec validate natural-indonesian-monthly-ui`
