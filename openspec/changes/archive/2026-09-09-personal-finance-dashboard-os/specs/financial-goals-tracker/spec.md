## ADDED Requirements

### Requirement: Financial Goals Management
The system SHALL support creating, editing, viewing, and deleting savings targets and financial goals:
1. Each goal SHALL contain `name`, `target_amount`, `current_amount`, optional `target_date`, optional `icon`, and optional `color`.
2. The system SHALL expose `GET /api/goals`, `POST /api/goals`, `PATCH /api/goals/{id}`, and `DELETE /api/goals/{id}`.

#### Scenario: Creating a savings goal
- **WHEN** a user posts a goal with name "Emergency Fund", target 30,000,000 IDR, and target date
- **THEN** the system persists the goal and calculates the percentage completed

### Requirement: Goal Milestone Progress & Monthly Pacing
The system SHALL compute the funding progress percentage and the required monthly contribution pace:
1. `percentage_completed`: `min(100, round((current_amount / target_amount) * 100))`.
2. `monthly_target_pace`: `max(0, round((target_amount - current_amount) / max(1, remaining_months)))`.

#### Scenario: Viewing goal progress card
- **WHEN** a user views a goal with 15,000,000 saved out of 30,000,000 with 5 months remaining
- **THEN** the goal card renders 50% completed with a progress bar and indicates a required pace of 3,000,000 IDR/month

### Requirement: Linking Ledger Transactions to Goals
The transaction recording system SHALL support an optional `goal_id`. Recording an income or transfer with `goal_id` SHALL increment `goals.current_amount`, and recording an expense with `goal_id` SHALL decrement it.

#### Scenario: Logging a contribution to a goal
- **WHEN** a user records a transfer of 1,000,000 IDR linked to a goal
- **THEN** the transaction is logged and the goal's `current_amount` increments by 1,000,000
