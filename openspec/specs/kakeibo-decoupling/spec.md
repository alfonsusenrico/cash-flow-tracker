# kakeibo-decoupling Specification

## Purpose
Keep transaction Kakeibo selection independent from category presentation, management and budget aggregation.

## Requirements

### Requirement: Clean Category Presentation
The system SHALL present category labels across all dropdowns, select elements, tables, and chart legends with only their display name and icon, without appending lifestyle classifications such as `(need)`, `(want)`, or `(saving)`.

#### Scenario: Selecting a category in Quick Capture
- **GIVEN** a category named "Makanan & Minuman"
- **WHEN** the user opens the Quick Capture modal
- **THEN** the category dropdown SHALL display "Makanan & Minuman" without any parenthetical classification suffix.

---

### Requirement: Independent Transaction Kakeibo Selection
The Quick Capture interface and the ledger edit form SHALL allow the user to choose any Kakeibo pillar (`need`, `want`, `culture`, `unexpected` or `saving`) independently of the selected category. Selecting a category SHALL NOT overwrite a pillar the user has chosen.

#### Scenario: Categorizing a discretionary dining expense
- **GIVEN** the Quick Capture modal is open for an expense
- **WHEN** the user selects the `[👑 Want]` pill
- **AND** selects the category "Makanan & Minuman"
- **THEN** the Kakeibo pill SHALL remain `[👑 Want]`
- **AND** submitting the transaction SHALL save `kakeibo_type = 'want'` with the chosen category ID.

#### Scenario: Recording a culture expense
- **GIVEN** the user records a sports court booking as an expense
- **WHEN** the user selects the culture pillar and submits
- **THEN** the transaction SHALL be saved with `kakeibo_type = 'culture'`.

#### Scenario: AI chooses an unexpected pillar
- **GIVEN** AI notification processing proposes `kakeibo = 'unexpected'` for a settled expense
- **WHEN** the backend records the transaction
- **THEN** the insert SHALL succeed with `kakeibo_type = 'unexpected'`
- **AND** the notification event SHALL end in `recorded`, not `failed`.

#### Scenario: Unknown pillar is rejected
- **WHEN** a client creates or updates a transaction or category with `kakeibo_type = 'luxury'`
- **THEN** the API SHALL respond with a 422 validation error
- **AND** no row SHALL change.

---

### Requirement: Unconstrained Category Management
The Category creation and edit interface SHALL NOT require or force users to assign a static Kakeibo classification to categories. Categories SHALL focus on identity (name, icon, color) and optional spending budget limits.

#### Scenario: Creating a category without Kakeibo constraint
- **GIVEN** the user opens the Create Category modal in the Insights page
- **WHEN** the user enters a category name and optional budget
- **THEN** the user SHALL be able to save the category without selecting a Kakeibo classification.

---

### Requirement: Pure Transaction-Level Kakeibo Aggregation
The system SHALL resolve every expense's effective Kakeibo pillar from the transaction's own pillar, otherwise its category's pillar, otherwise `need`. Ledger listing, pillar filtering, transaction detail and report aggregation SHALL use this shared rule. Category pillars SHALL remain optional.

#### Scenario: Aggregating mixed transactions under the same category
- **WHEN** a category contains a need expense of Rp 30.000 and a want expense of Rp 70.000
- **THEN** those amounts SHALL be counted under their respective effective pillars before applicable reimbursement offsets

#### Scenario: Transaction without its own pillar follows its category
- **WHEN** an expense has no transaction pillar but its category is `want`
- **THEN** the ledger and Kakeibo reports SHALL use `want`

#### Scenario: No pillar anywhere defaults to need
- **WHEN** an expense has no transaction or category pillar
- **THEN** it SHALL be counted as `need`

### Requirement: Kakeibo Pillar Vocabulary
The backend SHALL accept exactly the pillars `need`, `want`, `culture`, `unexpected` and `saving` wherever a transaction or category pillar is written: manual transactions, category defaults, notification resolution and AI interpretation. It SHALL reject any other value with a validation error.

#### Scenario: Category default accepts a new pillar
- **WHEN** a client updates a category with `kakeibo_type = 'culture'`
- **THEN** the category SHALL be stored with the `culture` pillar.

#### Scenario: Retired pillar name is rejected
- **WHEN** a client creates a transaction with `kakeibo_type = 'investment'`
- **THEN** the API SHALL respond with a 422 validation error and no row SHALL be created.
