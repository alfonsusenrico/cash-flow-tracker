# financial-goals-tracker Specification

## Purpose
TBD - created by archiving change personal-finance-dashboard-os. Update Purpose after archive.
## Requirements
### Requirement: Financial Goals Management
The system SHALL support creating, editing, viewing, and deleting savings targets and financial goals:
1. Each goal SHALL contain `name`, `target_amount`, `current_amount`, optional `target_date`, optional `icon`, and optional `color`.
2. The system SHALL expose `GET /api/goals`, `POST /api/goals`, `PATCH /api/goals/{id}`, and `DELETE /api/goals/{id}`.

#### Scenario: Creating a savings goal
- **WHEN** a user posts a goal with name "Emergency Fund", target 30,000,000 IDR, and target date
- **THEN** the system persists the goal and calculates the percentage completed

### Requirement: Goal Milestone Progress & Monthly Pacing
The system SHALL compute the funding progress percentage and required monthly contribution pace for individual goals:
1. `percentage_completed`: `min(100, round((current_amount / target_amount) * 100))`.
2. `monthly_target_pace`: `max(0, round((target_amount - current_amount) / max(1, remaining_months)))`.
3. The Goals view SHALL display individual target cards with their linked accounts and pacing progress, and SHALL NOT display an aggregated global target summary card that sums unrelated goals together.

#### Scenario: Viewing goals view without misleading aggregate banner
- **WHEN** the user navigates to the Goals page with an Emergency Fund goal and a Long-term Savings goal
- **THEN** both goals render as individual progress cards without an overarching combined target sum banner

### Requirement: Linking Ledger Transactions to Goals
The transaction recording system SHALL support an optional `goal_id`. Recording an income or transfer with `goal_id` SHALL increment `goals.current_amount`, and recording an expense with `goal_id` SHALL decrement it.

#### Scenario: Logging a contribution to a goal
- **WHEN** a user records a transfer of 1,000,000 IDR linked to a goal
- **THEN** the transaction is logged and the goal's `current_amount` increments by 1,000,000

### Requirement: Account-Backed Goals (Single or Multiple Linked Accounts)
The system SHALL allow users to link a savings goal to one or more physical accounts or pockets (such as money market funds, gold savings, deposits, or dedicated bank accounts):
1. A goal MAY be associated with zero, one, or multiple active accounts via `account_ids`.
2. When a goal has linked accounts, the goal's `current_amount` SHALL dynamically equal the sum of the non-redundant current balances of its linked accounts:
   - If both a parent account and one or more of its child pockets are linked, the child pocket balances SHALL NOT be double-counted; the parent's aggregate balance SHALL be used.
   - If only child pockets are linked without their parent account, their individual balances SHALL be summed normally.
3. The Goal creation and edit modals SHALL provide a multi-account selector displaying account names, types, and current balances without duplicate entries for child pockets, and SHALL calculate the combined balance in real time as accounts are selected or deselected.
4. Toggling a parent account in the selector SHALL toggle all its child pockets. Toggling a child pocket off SHALL uncheck the parent while preserving sibling selections.
5. When any linked account's balance changes (via income, expense, transfer, or balance reconciliation), any goal linked to that account SHALL automatically reflect the updated balance on subsequent queries.
6. If a goal is created without linked accounts, it SHALL allow entering or tracking progress via standalone manual contributions.
7. The Goal presentation view SHALL display contributing linked accounts without redundant child duplicate chips.

#### Scenario: Linking a single child pocket
- **WHEN** an authenticated user creates or edits a goal and selects only "Dana Darurat" (a child pocket with balance 3,500,000 IDR under parent "Jago")
- **THEN** the goal's `current_amount` is calculated as exactly 3,500,000 IDR and appears once in the selection checklist

#### Scenario: Linking parent account and child pockets simultaneously
- **WHEN** an authenticated user selects parent account "Bibit" (total aggregate balance 11,400,615 IDR) and also selects its child pockets "Sucorinvest" (5,712,112 IDR) and "TRIM Kas" (5,688,503 IDR)
- **THEN** the goal's `current_amount` is calculated as 11,400,615 IDR without double-counting the child pockets

#### Scenario: Linking multiple sibling child pockets without parent
- **WHEN** an authenticated user selects child pockets "Sucorinvest" (5,712,112 IDR) and "TRIM Kas" (5,688,503 IDR) without selecting parent "Bibit"
- **THEN** the goal's `current_amount` is calculated as the sum of the two pockets (11,400,615 IDR)

