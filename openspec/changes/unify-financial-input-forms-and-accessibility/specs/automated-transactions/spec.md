# Spec Delta

## ADDED Requirements

### Requirement: Complete Recurring Rule Management Interface
The recurring-rule interface SHALL support creating and editing every persisted rule field, deactivating and deleting rules, and clearly distinguishing transaction type, source, target, category, obligation, schedule, payroll inclusion, and automatic posting.
1. Weekly schedules SHALL provide an explicit weekday control.
2. Monthly schedules SHALL provide a day from 1 through 31.
3. Category choices SHALL match the selected income or expense type.
4. Payroll inclusion SHALL default to off and require explicit selection.
5. Changing rule type SHALL clear or replace incompatible target, category, and obligation values before submission.
6. Generic transfer account choices SHALL include only liquid accounts; investment positions SHALL be excluded with an explanation that position changes require Beli/Jual.

#### Scenario: Editing a weekly recurring expense
- **WHEN** a user edits an existing recurring expense and changes its weekday to Friday
- **THEN** the form loads the existing values, submits the Friday schedule, and displays the recalculated next due date

#### Scenario: Creating an ordinary recurring payment
- **WHEN** a user creates a recurring expense without choosing payroll inclusion
- **THEN** the rule is saved with `is_payroll_allocation = false`
