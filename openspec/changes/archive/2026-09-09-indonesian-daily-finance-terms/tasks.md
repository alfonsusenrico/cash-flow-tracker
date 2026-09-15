## 1. Global Navigation & Layout Shell Localization

- [x] 1.1 Update `frontend/src/components/layout/Sidebar.tsx` with natural Indonesian navigation labels (`Beranda`, `Transaksi`, `Analisis`, `Rekening & Saldo`, `Target & Tagihan`), quick CTA button (`Catat Transaksi`), and footer labels (`Preferensi`, `Keluar`).
- [x] 1.2 Update `frontend/src/components/layout/TopBar.tsx` with Indonesian screen titles, subtitles, payday cycle navigators (`Siklus Ini`, `Siklus Sebelumnya`, `Reset ke Sekarang`), and timeframe pills (`Siklus`, `30 Hari`, `90 Hari`).
- [x] 1.3 Update `frontend/src/components/layout/BottomNav.tsx` with friendly Indonesian tab labels for mobile.

## 2. Forms & Modals Localization

- [x] 2.1 Update `frontend/src/components/ui/QuickCaptureModal.tsx` with Indonesian form titles (`Catat Transaksi Baru`), transaction types (`Uang Keluar`, `Uang Masuk`, `Pindah Saldo`), field labels (`Nominal`, `Dari Rekening`, `Ke Rekening`, `Kategori`, `Catatan`), and action buttons (`Simpan Transaksi`, `Batal`).
- [x] 2.2 Update `frontend/src/components/ui/SettingsModal.tsx` with Indonesian labels (`Pengaturan Akun & Keuangan`, `Tanggal Gajian`, `Mata Uang`, `Sembunyikan Saldo`, `Kunci Bot Telegram`).
- [x] 2.3 Update `frontend/src/app/auth/login/page.tsx` with friendly Indonesian login / register copy (`Masuk ke Akun Anda`, `Nama Pengguna`, `Kata Sandi`, `Masuk`).

## 3. Core Dashboard & Screen Content Localization

- [x] 3.1 Update `frontend/src/app/page.tsx` and `frontend/src/components/dashboard/*` to use natural terms: `Batas Belanja Aman Hari Ini`, `Pengeluaran Hari Ini`, `Laju Pengeluaran`, `Total Saldo Tersedia`, `Uang Masuk`, `Uang Keluar`, `Transaksi Terakhir`.
- [x] 3.2 Update `frontend/src/app/ledger/page.tsx` (Transactions) with Indonesian search placeholders, filter pills, table columns (`Tanggal`, `Keterangan`, `Kategori`, `Rekening`, `Nominal`), and action buttons.
- [x] 3.3 Update `frontend/src/app/insights/page.tsx` (Analytics) with Indonesian chart legends, breakdown titles, and budget variance labels.
- [x] 3.4 Update `frontend/src/app/accounts/page.tsx` (Accounts) and `frontend/src/app/goals/page.tsx` (Goals & Debts) with friendly Indonesian vault, goal, and debt payoff terms.

## 4. Verification & Testing

- [x] 4.1 Run frontend type-check (`npm run type-check`) and linter (`npm run lint`).
- [x] 4.2 Run frontend production build (`npm run build`).
- [x] 4.3 Rebuild and restart Docker frontend container (`docker compose up -d --build frontend`).
- [x] 4.4 Validate OpenSpec change integrity with `openspec validate indonesian-daily-finance-terms`.
