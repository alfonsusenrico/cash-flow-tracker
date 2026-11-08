# monthly-navigation-stepper Specification

## Purpose
Provide a month-and-year navigation stepper for historical financial views without competing timeframe controls.

## Requirements

### Requirement: Month-and-Year Stepper Navigation
Every page whose figures depend on a selected period (Transaksi, Analisis and Laporan, and any page not yet redesigned that reads the shared period) SHALL show a period stepper in its own top-bar controls. It names the active salary cycle or calendar month in the interface language and the account region's date style, with chevrons to step and a reset to the current period. Beranda SHALL NOT show a stepper.

#### Scenario: User views active month
- **WHEN** the user opens Transaksi on the calendar-month basis
- **THEN** the stepper in the page's top-bar controls displays the current month and year in the interface language (e.g. "Nov 2026" in both Indonesian and English)

#### Scenario: Navigating to previous months
- **WHEN** the user activates the left chevron on Transaksi
- **THEN** the active period shifts backward (e.g. from "Nov 2026" to "Okt 2026", or "Oct 2026" in English), the page refreshes to that period, a "Bulan Ini" ("This month") reset appears, and Analisis opened next shows the same period

#### Scenario: Stepper on an untranslated page
- **WHEN** an English account opens Transaksi before its phase translates it
- **THEN** the stepper SHALL read in English while the page content stays Indonesian

#### Scenario: Beranda has no stepper
- **WHEN** the user opens Beranda
- **THEN** no period stepper is shown, and Beranda describes the current salary cycle or current month as defined by `beranda-cycle-summary`

### Requirement: Removal of Dual-Timeframe Rolling Pills
The application SHALL NOT present dual rolling timeframe selector pills (`[ 30 Hari | 90 Hari ]`) in the workspace top bar, unifying all views under monthly statement or salary-cycle accounting.

#### Scenario: Clean TopBar presentation
- **WHEN** viewing the workspace top bar on desktop or mobile
- **THEN** the 30d/90d pill switcher is absent, leaving the page title and subtitle, any page-specific period stepper, search, theme, notifications, "+ Catat" and the account menu (with the balance-privacy choice)
