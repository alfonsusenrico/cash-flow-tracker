## MODIFIED Requirements

### Requirement: Calendar Month Window
Dashboard overview and analytics SHALL accept `timeframe=month`, meaning the calendar month of today shifted by `cycle_offset` months, labelled in the account's language (for example "Oktober 2026" in Indonesian, "October 2026" in English), compared with the previous calendar month.

#### Scenario: Month boundaries
- **WHEN** the month window is computed for 10 February 2026, for 31 January 2026 with offset −1, and for 29 February 2024
- **THEN** it SHALL be 1–28 February 2026 "Februari 2026", 1–31 December 2025 "Desember 2025" and 1–29 February 2024.

#### Scenario: One window for report and ledger
- **GIVEN** expenses of Rp 45.000 inside last month and Rp 99.000 and Rp 11.000 one minute outside it
- **WHEN** analytics are requested with `timeframe=month&cycle_offset=-1` and the ledger with `period=month&period_offset=-1` for that category
- **THEN** both SHALL show only the Rp 45.000 row, with the same label.

#### Scenario: English label
- **WHEN** an English account requests analytics for February 2026
- **THEN** the window SHALL be the same dates, labelled "February 2026"
