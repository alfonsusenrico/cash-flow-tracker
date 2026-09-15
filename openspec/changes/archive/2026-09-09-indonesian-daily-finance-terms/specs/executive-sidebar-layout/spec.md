## MODIFIED Requirements

### Requirement: Persistent Left-Hand Executive Sidebar Navigation
The web dashboard SHALL replace the top tab navigation bar with a persistent left-hand sidebar navigation bar containing:
1. Brand header with application emblem and wordmark ("CashFlow").
2. Core navigation links with icons and active route highlighting rendered in simple, everyday Bahasa Indonesia:
   - `Beranda` (`/`)
   - `Transaksi` (`/ledger`)
   - `Analisis` (`/insights`)
   - `Rekening & Saldo` (`/accounts`)
   - `Target & Tagihan` (`/goals`)
3. Primary action trigger button (`+ Catat Transaksi` / `N`).
4. Bottom user section containing username, currency code (`IDR`), privacy balance toggle, theme toggle, and logout button (`Keluar`).

#### Scenario: Navigating via sidebar
- **WHEN** the user clicks any route in the left sidebar
- **THEN** the active indicator transitions to the selected route and the main workbench area renders the corresponding view without page reloading
