## ADDED Requirements

### Requirement: Custom Category Creation & Management
The system SHALL provide full category lifecycle management allowing users to create, update, archive, and query custom categories:
1. Category entities SHALL include `name`, `kind` (`income` or `expense`), `is_primary` (boolean: true for `Pokok`, false for `Opsional`), optional `color`, optional `icon`, and optional `monthly_budget`.
2. The system SHALL expose `GET /api/categories`, `POST /api/categories`, `PATCH /api/categories/{id}`, and `DELETE /api/categories/{id}` (soft-delete/archive).
3. The category listing SHALL allow filtering by `kind` (`expense` or `income`) and support searching active categories.

#### Scenario: Creating a custom expense category
- **WHEN** a user creates a category with name "Hobi", kind "expense", is_primary false, and monthly budget 500,000 IDR
- **THEN** the category is persisted with the "Opsional" tag and appears in the expense category selection list

### Requirement: Category Budget Variance Filtering
The `/insights` category budget matrix SHALL strictly filter and display categories of kind `expense`:
1. Income categories (e.g. "Gaji", "Pendapatan Lain") SHALL NOT be displayed in the Category Spending Variance or Budget table.
2. Users SHALL be able to click directly on any category's budget cell to edit or set its `monthly_budget` ceiling via an inline modal.

#### Scenario: Viewing the category budget matrix
- **WHEN** the user views the Category Budget table on the `/insights` page
- **THEN** only expense categories are shown, each with their `Pokok` or `Opsional` badge, actual spending, and budget ceiling
