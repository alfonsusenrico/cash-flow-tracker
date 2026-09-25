# Spec Delta

## MODIFIED Requirements

### Requirement: Custom Category Creation & Management
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
