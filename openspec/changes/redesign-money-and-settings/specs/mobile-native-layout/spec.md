## ADDED Requirements

### Requirement: Phone Rekening & Kantong Composition
At 760 px and below, Rekening & Kantong SHALL use a composition built for phones: the Total aset and Kekayaan bersih summary first, then the "Semua / Siap pakai / Investasi" switch, then the account list with pockets indented under their account and investments grouped by platform, each row's kind and balance on one line below its name. Every row SHALL open its actions in a bottom sheet.

#### Scenario: Rekening on a phone
- **WHEN** the owner opens `/accounts` at 390 px
- **THEN** the summary SHALL come first, no table SHALL scroll sideways, no page SHALL overflow horizontally, and tapping a pocket SHALL open its bottom sheet with its actions

#### Scenario: Closing a sheet returns focus
- **WHEN** a keyboard or switch user closes a row's bottom sheet with its close control or Escape
- **THEN** focus SHALL return to the row that opened it

### Requirement: Phone Target & Tagihan Composition
At 760 px and below, Target & Tagihan SHALL show Ketahanan Dana first, then Target tabungan, Utang and the bills up to the next salary, each goal and debt opening a bottom sheet.

#### Scenario: Target & Tagihan on a phone
- **WHEN** the owner opens `/goals` at 390 px
- **THEN** Ketahanan Dana SHALL come first and each goal row SHALL show its progress text without truncating the amounts

### Requirement: Phone Page Actions and Targets
On phones, the page actions of the money pages ("Tambah rekening", "Tambah target", "Tambah utang") SHALL stay reachable above the phone tab bar, every target SHALL be at least 44 by 44 CSS pixels, and a focused control SHALL not be hidden by the sticky top bar or the tab bar.

#### Scenario: Adding a target on a phone
- **WHEN** the owner scrolls to the end of `/goals` at 390 px and tabs to "Tambah target"
- **THEN** the control SHALL be fully visible above the tab bar with a visible focus ring, and activating it SHALL open the goal form as a bottom sheet
