## MODIFIED Requirements

### Requirement: Persistent Left-Hand Executive Sidebar Navigation
The web dashboard SHALL provide a persistent left-hand sidebar navigation bar dedicated to route navigation:
1. The sidebar SHALL NOT contain redundant branding titles, avatars ("CashFlow Financial OS"), or redundant quick transaction entry buttons. Quick transaction entry (`+ Catat Transaksi` / `N`) SHALL reside exclusively in the top global header.
2. The sidebar SHALL contain core navigation links with icons and active route highlighting rendered in simple, everyday Bahasa Indonesia:
   - `Beranda` (`/`)
   - `Transaksi` (`/ledger`)
   - `Analisis` (`/insights`)
   - `Rekening & Saldo` (`/accounts`)
   - `Target & Tagihan` (`/goals`)
3. The bottom user identity section SHALL contain the user's avatar initials, username, currency code, settings trigger, and integrated logout button (`Keluar`).
4. Privacy balance toggle and theme toggle buttons SHALL NOT be duplicated in the sidebar footer; they SHALL reside exclusively in the top global header.

#### Scenario: Navigating via streamlined sidebar
- **WHEN** the user navigates between views using the left sidebar
- **THEN** the sidebar displays only the route navigation links and bottom user identity with logout, without redundant branding banners or redundant "+ Catat Transaksi" buttons

#### Scenario: Logging out from user identity card
- **WHEN** the user clicks the logout icon within the sidebar user identity card
- **THEN** the active session terminates and redirects to the login screen
