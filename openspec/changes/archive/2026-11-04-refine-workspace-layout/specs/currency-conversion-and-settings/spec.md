## ADDED Requirements

### Requirement: Help and Phone Status in Pengaturan
Pengaturan SHALL hold what left the sidebar: "HP & perangkat" SHALL show the companion status at its top, and "Tampilan" SHALL hold "Bantuan & pintasan" with the keyboard shortcuts and help. The `?` key SHALL still open the same help from every page while shortcuts are on.

#### Scenario: Finding help without the sidebar entry
- **WHEN** the owner opens Pengaturan · Tampilan
- **THEN** "Bantuan & pintasan" SHALL list the shortcuts and open the help, and pressing `?` on Beranda SHALL open the same help

#### Scenario: Phone status in Pengaturan
- **WHEN** the owner opens Pengaturan · HP & perangkat with a paired phone that last reported yesterday at 16.05
- **THEN** the section SHALL start with "HP terhubung · Terakhir menangkap kemarin 16.05" as text
