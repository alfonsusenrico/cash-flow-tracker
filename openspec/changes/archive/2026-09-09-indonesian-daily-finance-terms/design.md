## Context

The application is built for regular users tracking personal daily cash flow. While the underlying engine and architecture are robust, the user interface currently employs technical financial terminology ("Executive Cockpit", "Full Ledger", "Spending Cadence", "Budget Variance", "Liquid Net Worth", "Runway", "Liability Payoffs"). Furthermore, English is currently the hardcoded UI language. The project owner has explicitly mandated:
1. All user-facing labels must use simple, natural, everyday terms that regular individuals easily understand.
2. The default language for the application is **Bahasa Indonesia**.
3. The terminology should mirror familiar Indonesian consumer fintech standards such as **Bibit**, **Bank Jago**, and **SeaBank**.

## Goals / Non-Goals

**Goals:**
- Translate and normalize all customer-facing text across all screens, modals, navigation, buttons, and notifications to natural Bahasa Indonesia.
- Replace jargon with friendly, daily personal finance terms:
  - `Overview` -> `Beranda` / `Ringkasan`
  - `Transactions` -> `Catatan Transaksi` / `Riwayat`
  - `Analytics` -> `Laporan & Analisis`
  - `Vault & Accounts` -> `Kantong & Rekening` / `Daftar Rekening`
  - `Goals & Debts` -> `Target & Tagihan`
  - `Safe to spend today` -> `Batas Belanja Aman Hari Ini`
  - `Days to payday` -> `Hari Menuju Gajian`
  - `Cash In` -> `Uang Masuk`
  - `Cash Out` -> `Uang Keluar`
  - `Transfer` -> `Pindah Saldo`
  - `Net Cash Flow` -> `Sisa Arus Kas`
  - `Record / + Record Entry` -> `Catat Transaksi`
- Ensure all input placeholders, empty states, error notices, confirmations, and date formats use natural Indonesian phrasing (e.g. `Senin, 9 September`, `Bulan Ini`, `Siklus Ini`).

**Non-Goals:**
- Changing database schema or backend API endpoints.
- Introducing a complex multi-lingual i18n localization engine with JSON translation files; since Bahasa Indonesia is the authoritative default project language, natural Indonesian copy will be directly baked into the UI components cleanly.

## Decisions

### 1. Direct Indonesian Copy Standard
- **Decision:** Write natural, friendly Bahasa Indonesia directly in the React components rather than adding a heavy third-party i18n abstraction library (e.g. `next-intl`, `react-i18next`).
- **Rationale:** The application default language is definitively Bahasa Indonesia. Keeping copy direct keeps bundle size minimal, eliminates translation key lookup overhead, and avoids developer friction.

### 2. Indonesian Personal Finance Terminology Mapping (Bank Jago / Bibit Standard)
- **Navigation & Layout:**
  - Overview -> `Beranda` (Subtitle: `Pantauan Keuangan & Arus Kas Harian`)
  - Transactions -> `Transaksi` (Subtitle: `Riwayat & Catatan Keuangan Lengkap`)
  - Analytics -> `Analisis` (Subtitle: `Laporan Pengeluaran & Anggaran`)
  - Accounts -> `Rekening & Saldo` (Subtitle: `Daftar Rekening, Dompet Digital & Tunai`)
  - Goals & Debts -> `Target & Tagihan` (Subtitle: `Target Tabungan & Rencana Pembayaran`)
- **Quick Capture Form:**
  - Modal title: `Catat Transaksi Baru`
  - Types: `Uang Keluar` (Expense), `Uang Masuk` (Income), `Pindah Saldo` (Transfer)
  - Fields: `Nominal Uang`, `Kategori`, `Dari Rekening`, `Ke Rekening`, `Catatan (Opsional)`, `Tanggal Transaksi`
  - Buttons: `Simpan Transaksi`, `Batal`
- **Pulse & Overview Metrics:**
  - `Batas Belanja Hari Ini` (Safe to spend today)
  - `Pengeluaran Hari Ini` (Spent today)
  - `Laju Pengeluaran Siklus Ini` (Cycle pace)
  - `Total Saldo Tersedia` (Total liquid net worth)
  - `Transaksi Terakhir` (Recent activity)
- **Settings Modal:**
  - Title: `Pengaturan Akun & Keuangan`
  - Fields: `Tanggal Gajian Tiap Bulan`, `Mata Uang`, `Sembunyikan Saldo (Mode Privasi)`, `Kunci Integrasi Telegram`

## Risks / Trade-offs

- **[Button & Label Truncation]** -> Indonesian phrases can occasionally be slightly longer than English words (e.g., `Uang Keluar` vs `Expense`). Ensure button flex-wrap, whitespace styling, and container padding remain clean and responsive on mobile viewports.
