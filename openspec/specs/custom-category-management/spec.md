# custom-category-management Specification

## Purpose
Allow users to manage transaction categories and apply category budget variance filters.

## Requirements

### Requirement: Custom Category Creation & Management
The system SHALL provide full category lifecycle management allowing users to create, update, archive, and query custom categories.

#### Scenario: Required behavior and constraints
- **WHEN** an owner manages custom transaction categories
- **THEN** the following required behavior and constraints hold:

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

### Requirement: Snack and Café Default Category
Every user SHALL have the expense category "Jajan & Nongkrong" with the Kakeibo pillar `want`, separate from daily meals. The migration SHALL create it only where the user has no expense category of that name, SHALL leave an existing one unchanged, and SHALL NOT move any existing transaction into it.

#### Scenario: Existing user without the category
- **GIVEN** a user whose food spending is all in "Makanan & Minuman"
- **WHEN** the migration runs
- **THEN** the user SHALL have the expense category "Jajan & Nongkrong" with pillar `want`
- **AND** every existing transaction SHALL keep its category.

#### Scenario: Existing category is kept
- **GIVEN** a user who already has an expense category "Jajan & Nongkrong" with pillar `need`
- **WHEN** the migration runs
- **THEN** the category SHALL keep pillar `need`.

#### Scenario: New user defaults
- **WHEN** a new user registers
- **THEN** the default categories SHALL include "Jajan & Nongkrong" with pillar `want`.
