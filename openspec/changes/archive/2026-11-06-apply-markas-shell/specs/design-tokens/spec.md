## ADDED Requirements

### Requirement: Interface Icons
Interface icons SHALL be drawn on a 24 px grid with a 1.5 px stroke, round caps and joins and `currentColor`, rendered at 16 px in controls, rows and navigation and at 20 px in the phone header and tab bar. The shell's icons SHALL use the prototype's drawings. Icons SHALL be hidden from assistive technology unless they are the only label, which their control then names.

#### Scenario: Shell icons measured
- **WHEN** the sidebar, top bar, phone header and tab bar are captured at 1440 and 390 px
- **THEN** every icon `svg` SHALL have a 1.5 px stroke, the nav and top-bar icons SHALL be 16 px and the phone header and tab bar icons 20 px

#### Scenario: Icon-only control
- **WHEN** a screen reader reaches the bell or the "…" button
- **THEN** it SHALL announce the control's name, for example "Notifikasi, 2 baru" or "Tindakan lain", and not the icon
