## MODIFIED Requirements

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
