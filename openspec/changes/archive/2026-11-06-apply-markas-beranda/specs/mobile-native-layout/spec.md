## MODIFIED Requirements

### Requirement: Dedicated Mobile Home Information Architecture
Below 768 px Beranda SHALL be one column in the phone order: the hero with Pengeluaran and Ditabung & investasi, "Perlu tindakan", "Aktivitas terbaru" with two-line rows, and the context panel. It SHALL use phone-sized targets.

#### Scenario: Required behavior and constraints
- **WHEN** the application renders Beranda below 768 pixels
- **THEN** it SHALL start with the shell's phone header (brand, search, "Sembunyikan saldo" and the bell) and then the title block with the date and the period line; theme, the account's actions and the companion status SHALL NOT be in the header
- **AND** the first block SHALL be "Aman dipakai sampai gajian" with its line, the split bar, Pengeluaran and Ditabung & investasi, and its "Rincian" opening as a bottom sheet
- **AND** "Perlu tindakan" SHALL follow, then "Aktivitas terbaru", then Kekayaan bersih, Ketahanan Dana and Kakeibo
- **AND** Kakeibo SHALL make all four spending pillars and "Belum diklasifikasi" identifiable
- **AND** secondary analytical charts SHALL remain on Analisis, and the phone tab bar SHALL never cover the last block

#### Scenario: Viewing home dashboard on mobile phone
- **WHEN** a user opens Beranda at 390 px or 320 px
- **THEN** the interface shows the hero, "Perlu tindakan", the activity and the panel in one column without the sidebar, without horizontal scrolling, with every row and button at least 44 by 44 CSS pixels and every inline link at least 24 by 24

#### Scenario: Phone figures match desktop
- **WHEN** the same owner opens Beranda at 390 px and at 1440 px at the same moment
- **THEN** Pengeluaran, Ditabung & investasi, masuk, the waiting amount and the safe amount SHALL be identical in both layouts, with masuk, Terjadwal and the waiting amount read in the Rincian at 390 px
