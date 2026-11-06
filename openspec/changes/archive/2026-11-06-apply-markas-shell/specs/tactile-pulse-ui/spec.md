## MODIFIED Requirements

### Requirement: 3-Screen Information Architecture
The web application SHALL offer one short page list shared by the desktop sidebar, the phone tab bar and the Lainnya sheet, in three groups: HARIAN, RINGKASAN and SIMPANAN.

#### Scenario: Required behavior and constraints
- **WHEN** the application renders navigation for the current viewport
- **THEN** the following required behavior and constraints hold:

  The web application SHALL offer one short page list:
  1. Pages, in this order: HARIAN `Beranda` `/`, `Tinjau` `/review`, `Transaksi` `/ledger`; RINGKASAN `Analisis` `/insights`, `Laporan` `/laporan`; SIMPANAN `Rekening & Kantong` `/accounts` and `Target & Tagihan` `/goals`, with `Pengaturan` (`/settings`) apart at the foot. There SHALL be no Tanya page and no separate Bantuan entry; help opens with `?`.
  2. From 768 px the list SHALL appear in the sidebar with its group labels (`executive-sidebar-layout`).
  3. Below 768 px a bottom tab bar SHALL offer Beranda, Tinjau, Catat (opens capture), Transaksi and Lainnya; Lainnya SHALL reach every other page in the same order.

#### Scenario: Navigating between executive dashboard screens
- **WHEN** a user selects any page from the sidebar or the phone tab bar
- **THEN** the page switches without a full page reload, the active page is marked, and the page-specific period selection of Transaksi, Analisis and Laporan is kept while moving between them

#### Scenario: Mobile responsive dashboard navigation
- **WHEN** a user opens the app below 768 px
- **THEN** the navigation is the bottom tab bar with the Tinjau count and the Lainnya sheet
- **AND** the sidebar is hidden and no hamburger slide-over menu exists

#### Scenario: Same order everywhere
- **WHEN** the sidebar at 1440 px and the Lainnya sheet at 390 px are read
- **THEN** the pages SHALL appear in the same relative order, with Analisis before Laporan and Rekening & Kantong before Target & Tagihan in both
