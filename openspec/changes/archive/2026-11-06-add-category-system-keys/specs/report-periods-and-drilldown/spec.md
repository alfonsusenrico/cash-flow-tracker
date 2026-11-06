## MODIFIED Requirements

### Requirement: Complete Category Breakdown
Overview `categories` and analytics `category_variance` SHALL list active expense categories, archived categories with spending in the window (`is_archived: true`) and, when uncategorised spending exists, a "Tanpa kategori" bucket (`id: null`, `is_uncategorized: true`). Spending in categories reports exclude (keys `internal_movement` and `investment`, whatever the name, or excluded from budget) SHALL count neither in the breakdown nor in total outflow, so the breakdown sums to `total_outflow`.

#### Scenario: Breakdown adds up
- **GIVEN** a cycle with the shared-dinner figures, Rp 70.000 uncategorised, Rp 30.000 in a category archived afterwards, and spending in "Investasi" and "Internal Movement"
- **WHEN** analytics and overview are requested
- **THEN** the sum of category `spent` SHALL equal `total_outflow` Rp 400.000
- **AND** "Tanpa kategori" SHALL show Rp 70.000 and the archived category Rp 30.000
- **AND** neither "Investasi" nor "Internal Movement" SHALL be listed.

#### Scenario: Excluded categories after a rename
- **GIVEN** the same cycle after the owner renamed "Investasi" to "Investments"
- **WHEN** analytics and overview are requested
- **THEN** `total_outflow` SHALL still be Rp 400.000 and "Investments" SHALL NOT be listed.
