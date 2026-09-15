# kakeibo-decoupling Specification

## Purpose
TBD - created by archiving change decouple-kakeibo-from-categories. Update Purpose after archive.
## Requirements
### Requirement: Clean Category Presentation
The system SHALL present category labels across all dropdowns, select elements, tables, and chart legends with only their display name and icon, without appending lifestyle classifications such as `(need)`, `(want)`, or `(saving)`.

#### Scenario: Selecting a category in Quick Capture
- **GIVEN** a category named "Makanan & Minuman"
- **WHEN** the user opens the Quick Capture modal
- **THEN** the category dropdown SHALL display "Makanan & Minuman" without any parenthetical classification suffix.

---

### Requirement: Independent Transaction Kakeibo Selection
The Quick Capture interface SHALL allow the user to choose `need`, `want`, or `saving` independently of the selected category, and selecting a category SHALL NOT overwrite the user's selected Kakeibo pillar.

#### Scenario: Categorizing a discretionary dining expense
- **GIVEN** the Quick Capture modal is open for an expense
- **WHEN** the user selects the `[👑 Want]` pill
- **AND** selects the category "Makanan & Minuman"
- **THEN** the Kakeibo pill SHALL remain `[👑 Want]`
- **AND** submitting the transaction SHALL save `kakeibo_type = 'want'` with the chosen category ID.

---

### Requirement: Unconstrained Category Management
The Category creation and edit interface SHALL NOT require or force users to assign a static Kakeibo classification to categories. Categories SHALL focus on identity (name, icon, color) and optional spending budget limits.

#### Scenario: Creating a category without Kakeibo constraint
- **GIVEN** the user opens the Create Category modal in the Insights page
- **WHEN** the user enters a category name and optional budget
- **THEN** the user SHALL be able to save the category without selecting a Kakeibo classification.

---

### Requirement: Pure Transaction-Level Kakeibo Aggregation
The backend Kakeibo analytics engine SHALL aggregate expenses based primarily on `transactions.kakeibo_type` (defaulting to `'need'` if null), without enforcing or requiring category-level Kakeibo classifications.

#### Scenario: Aggregating mixed transactions under the same category
- **GIVEN** two expense transactions under category "Makanan & Minuman": one marked `need` for Rp 30.000 and one marked `want` for Rp 70.000
- **WHEN** the Kakeibo breakdown endpoint is executed
- **THEN** Rp 30.000 SHALL be counted in `need_spent`
- **AND** Rp 70.000 SHALL be counted in `want_spent`.

