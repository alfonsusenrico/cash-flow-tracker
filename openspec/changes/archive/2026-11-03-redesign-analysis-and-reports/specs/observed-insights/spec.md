## ADDED Requirements

### Requirement: Kakeibo Comparison Block
The period comparison SHALL also report a `kakeibo` block with the six Kakeibo buckets for the current and the previous compared windows, in which saving includes net fresh saving through linked movements, computed by the same rule as the dashboard's Kakeibo breakdown.

#### Scenario: Kakeibo buckets with saving transfers
- **GIVEN** Rp 500.000 moved from "Bank Utama" into a goal pocket in the current compared window, and none in the previous one
- **WHEN** the comparison is requested
- **THEN** `kakeibo.current.saving` SHALL include Rp 500.000, `kakeibo.previous.saving` SHALL not, the existing `pillars` entries SHALL keep their values, and `kakeibo.current` SHALL equal the dashboard's Kakeibo breakdown of the same window
