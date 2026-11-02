# Spec Delta

## MODIFIED Requirements

### Requirement: Dedicated Mobile Home Information Architecture
At 760 px and below the web app SHALL render a dedicated single-column Beranda built for phones, not the desktop layout stacked.

#### Scenario: Required behavior and constraints
- **WHEN** the application renders Beranda at 760 pixels or narrower
- **THEN** it SHALL show a compact sticky header with the date, the period line, theme and the account menu, and the companion status under it
- **AND** the first card SHALL be "Aman dipakai hari ini" (or its unavailable state) with the remaining amount and days, a labelled Dipakai / Terjadwal / Aman split and the waiting amount, each figure explainable as a bottom sheet
- **AND** "Kerjakan dulu" SHALL follow, then the period's keluar and masuk, Kekayaan bersih, today's transactions and one insight
- **AND** Kakeibo SHALL make all five classifications and "Belum diklasifikasi" identifiable where it is shown
- **AND** secondary analytical charts SHALL remain on Analisis, and the phone tab bar SHALL never cover the last card

#### Scenario: Viewing home dashboard on mobile phone
- **WHEN** a user opens Beranda at 390 px
- **THEN** the interface shows the safe-to-spend card, "Kerjakan dulu", the period figures and today's activity in one column without the sidebar, without horizontal scrolling, and with every target at least 24 by 24 CSS pixels

#### Scenario: Phone figures match desktop
- **WHEN** the same owner opens Beranda at 390 px and at 1440 px at the same moment
- **THEN** keluar, masuk, the waiting amount, the remaining amount and the safe-to-spend figure SHALL be identical in both layouts
