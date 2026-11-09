# monthly-navigation-stepper Specification

## Purpose
Provide a month-and-year navigation stepper for historical financial views without competing timeframe controls.

## Requirements

### Requirement: Removal of Dual-Timeframe Rolling Pills
The application SHALL NOT present dual rolling timeframe selector pills (`[ 30 Hari | 90 Hari ]`) in the workspace top bar, unifying all views under monthly statement or salary-cycle accounting.

#### Scenario: Clean TopBar presentation
- **WHEN** viewing the workspace top bar on desktop or mobile
- **THEN** the 30d/90d pill switcher is absent, leaving the page title and subtitle, any page-specific period stepper, search, theme, notifications, "+ Catat" and the account menu (with the balance-privacy choice)

### Requirement: Period Stepper
Every page whose figures depend on a selected period (Transaksi, Analisis and Laporan) SHALL show a period stepper in its title block: two chevron buttons around the period's name in the interface language and the region's date style, and a reset to the current period once stepped back. Beranda SHALL NOT show a stepper.

#### Scenario: User views active month
- **WHEN** the user opens Transaksi on the calendar-month basis
- **THEN** the stepper in the page's title block displays the current month and year in the interface language (e.g. "Nov 2026" in both Indonesian and English)

#### Scenario: Navigating to previous months
- **WHEN** the user activates the left chevron on Transaksi
- **THEN** the active period shifts backward (e.g. from "Nov 2026" to "Okt 2026", or "Oct 2026" in English), the page refreshes to that period, a "Bulan ini" ("This month") reset appears, and Analisis opened next shows the same period

#### Scenario: Cycle range
- **WHEN** the user opens Transaksi or Analisis on the salary-cycle basis for the cycle of 23 October to 24 November
- **THEN** the stepper SHALL read "Siklus 23 Okt – 24 Nov" in Indonesian and "Cycle 23 Oct – 24 Nov" in English, and the next chevron SHALL be disabled on the current cycle

#### Scenario: Stepper look
- **WHEN** the stepper is shown at 1440 px
- **THEN** its chevrons SHALL be 36 × 36 px icon buttons (44 × 44 px below 768 px), its label 14 px medium at least 168 px wide, and it SHALL have no box, border or fill

#### Scenario: Beranda has no stepper
- **WHEN** the user opens Beranda
- **THEN** no period stepper is shown, and Beranda describes the current salary cycle or current month as defined by `beranda-cycle-summary`

#### Scenario: Report period name
- **WHEN** an English account opens the October 2026 report and then the cycle report of 25 September to 22 October
- **THEN** the stepper SHALL read "October 2026" and then "Cycle 25 Sep – 22 Oct", formatted from the report's dates
