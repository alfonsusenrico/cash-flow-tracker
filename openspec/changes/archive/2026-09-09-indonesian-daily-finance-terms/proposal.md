## Why

The application currently uses heavy financial and technical jargon (e.g. "Executive Cockpit", "Full Ledger", "Spending Cadence & Budget Variance", "Liquid Net Worth & Vault", "Liability Payoffs", "Safe to Spend Today"). For everyday users tracking their daily cash flow, these complex terms create unnecessary friction and confusion. By setting Bahasa Indonesia as the default language and using natural, intuitive, everyday terms (inspired by familiar Indonesian consumer finance apps like Bibit, Bank Jago, and SeaBank), the app becomes effortless, welcoming, and immediately understood by regular users.

## What Changes

- **Default Bahasa Indonesia UI Copy**: Transition all UI headers, navigation links, buttons, modal titles, field labels, empty states, and notifications to natural Bahasa Indonesia by default.
- **Friendly Daily Finance Terminology**:
  - `Overview / Executive Cockpit` -> `Ringkasan Keuangan` / `Beranda`
  - `Transactions / Full Ledger` -> `Catatan Transaksi` / `Riwayat Transaksi`
  - `Analytics / Spending Cadence` -> `Analisis & Laporan Pengeluaran`
  - `Vault & Accounts / Liquid Net Worth` -> `Kantong & Rekening` / `Total Saldo`
  - `Goals & Debts / Liability Payoffs` -> `Target Tabungan & Tagihan`
  - `Record Cash Movement / Quick Capture` -> `Catat Transaksi`
  - `Cash In / Cash Out / Transfer` -> `Uang Masuk` / `Uang Keluar` / `Pindah Saldo`
  - `Safe to Spend Today` -> `Batas Belanja Aman Hari Ini`
  - `Days to Payday` -> `Hari Menuju Gajian`
  - `Source / Destination Account` -> `Dari Rekening` / `Ke Rekening`
  - `Settings` -> `Pengaturan`
  - `Login / Sign In` -> `Masuk ke Akun`
- **Consistent Indonesian Date & Number Formatting**: Display Indonesian date strings, cycle indicators (e.g., `Siklus Ini`, `Siklus Sebelumnya`), and currency formatters without alienating jargon.

## Capabilities

### New Capabilities
- `indonesian-daily-finance-ui`: Establishes the standard Indonesian dictionary of everyday personal finance terms and applies it across navigation, forms, modals, status cards, and analytics.

### Modified Capabilities
- `executive-sidebar-layout`: Updates sidebar navigation labels, tooltips, and header titles to use friendly Indonesian terminology.
- `tactile-pulse-ui`: Updates the pulse cards, allowance metrics, transaction pills, and quick capture modals to use simple Indonesian labels.

## Impact

- **Frontend**:
  - `frontend/src/components/layout/Sidebar.tsx`: Navigation items, user profile labels, drawer labels.
  - `frontend/src/components/layout/TopBar.tsx`: Page titles, subtitles, cycle navigation buttons, timeframe selectors.
  - `frontend/src/components/layout/BottomNav.tsx`: Mobile bottom bar tab labels.
  - `frontend/src/components/ui/QuickCaptureModal.tsx`: Transaction entry form labels, type tabs, field hints, buttons.
  - `frontend/src/components/ui/SettingsModal.tsx`: Preference labels, payday settings, privacy toggles.
  - `frontend/src/app/page.tsx` & `frontend/src/components/dashboard/*`: Safe-to-spend cards, monthly pace, recent feed, action buttons.
  - `frontend/src/app/ledger/page.tsx`: Search inputs, filter dropdowns, table headers, action menus.
  - `frontend/src/app/insights/page.tsx`: Spending chart legends, category breakdown headers, period pickers.
  - `frontend/src/app/accounts/page.tsx`: Total balance cards, account list, transfer modal.
  - `frontend/src/app/goals/page.tsx`: Savings goal headers, debt cards, progress indicators.
  - `frontend/src/app/auth/login/page.tsx`: Login credentials form labels, submit buttons.
- **Backend / Database**: None.
