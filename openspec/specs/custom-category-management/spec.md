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
  1. Category forms SHALL expose `name`, `kind` (`income` or `expense`), Kakeibo pillar where applicable, `is_primary`, optional color, optional icon, and optional monthly budget.
  2. Income categories SHALL not present expense-only budget or Kakeibo controls unless the underlying contract supports them.
  3. Existing values SHALL load when editing and every persisted attribute SHALL remain editable where compatible.
  4. Color and icon choices SHALL expose names and selected states without relying on appearance alone.
  5. The system SHALL expose `GET /api/categories`, `POST /api/categories`, `PATCH /api/categories/{id}`, and soft-delete/archive behavior.
  6. Transaction forms SHALL filter category choices by the selected transaction type.

#### Scenario: Creating an optional expense category
- **WHEN** a user creates "Hobi" as an expense, selects `want`, marks it optional, and sets a 500,000 IDR budget
- **THEN** all selected attributes are persisted and the category appears only in expense selections

#### Scenario: Creating an income category
- **WHEN** a user creates an income category named "Bonus"
- **THEN** it appears in income transaction forms and not in expense-only budget tables

#### Scenario: Creating a custom expense category
- **WHEN** a user creates "Hobi" as an optional expense category with a 500,000 IDR monthly budget
- **THEN** the category is persisted with its selected classification and appears in expense category choices

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
