# financial-goals-tracker Specification

## Purpose
Define savings goals, progress pacing, ledger linkage and funding from one or more linked accounts.

## Requirements

### Requirement: Financial Goals Management
The system SHALL support creating, editing, viewing, and deleting savings targets and financial goals:
1. Each goal SHALL contain `name`, `target_amount`, `current_amount`, optional `target_date`, optional `icon`, and optional `color`.
2. The system SHALL expose `GET /api/goals`, `POST /api/goals`, `PATCH /api/goals/{id}`, and `DELETE /api/goals/{id}`.

#### Scenario: Creating a savings goal
- **WHEN** a user posts a goal with name "Emergency Fund", target 30,000,000 IDR, and target date
- **THEN** the system persists the goal and calculates the percentage completed

### Requirement: Goal Milestone Progress & Monthly Pacing
The system SHALL compute the funding progress percentage and required monthly contribution pace for individual goals.

#### Scenario: Required behavior and constraints
- **WHEN** the system calculates a goal milestone and contribution pace
- **THEN** the following required behavior and constraints hold:

  The system SHALL compute the funding progress percentage and required monthly contribution pace for individual goals:
  1. `percentage_completed`: `min(100, round((current_amount / target_amount) * 100))`.
  2. `monthly_target_pace`: `max(0, round((target_amount - current_amount) / max(1, remaining_months)))`.
  3. The Goals view SHALL display individual target cards with their linked accounts and pacing progress, and SHALL NOT display an aggregated global target summary card that sums unrelated goals together.

#### Scenario: Viewing goals view without misleading aggregate banner
- **WHEN** the user navigates to the Goals page with an Emergency Fund goal and a Long-term Savings goal
- **THEN** both goals render as individual progress cards without an overarching combined target sum banner

### Requirement: Account-Backed Goals (Single or Multiple Linked Accounts)
The system SHALL allow users to link a savings goal to one or more active physical accounts or pockets.

#### Scenario: Required behavior and constraints
- **WHEN** an owner links accounts or pockets to a savings goal or views its funding progress
- **THEN** the following required behavior and constraints hold:

  The system SHALL allow users to link a savings goal to one or more active physical accounts or pockets:
  1. A goal MAY be associated with zero, one, or multiple active user-owned accounts via `account_ids`; unknown, archived, or foreign accounts SHALL be rejected rather than ignored.
  2. When a goal has linked accounts, its displayed progress SHALL dynamically equal the sum of the non-redundant current balances of those accounts.
  3. Linking or unlinking accounts SHALL NOT create ledger transactions, copy balances into standalone progress, or otherwise change account balances.
  4. If both a parent and any children are linked, the parent's aggregate balance SHALL be counted once and child balances SHALL NOT be added again.
  5. If only children are linked, their balances SHALL be summed normally.
  6. Direct standalone progress adjustments SHALL be unavailable while accounts are linked.
  7. Every subsequent income, expense, movement, trade, or reconciliation affecting a linked account SHALL be reflected in goal progress on the next query without a goal-specific transaction.
  8. The Goal creation and edit modals SHALL provide a multi-account selector displaying account names, types, and current balances without duplicate entries for child pockets, and SHALL calculate the combined balance in real time as accounts are selected or deselected.
  9. Toggling a parent account in the selector SHALL toggle all its child pockets. Toggling a child pocket off SHALL uncheck the parent while preserving sibling selections.
  10. The goal view SHALL display contributing accounts without redundant child chips.

#### Scenario: Applying accounts to a goal without financial mutation
- **WHEN** a user links an account with a 3,500,000 IDR balance to an existing goal
- **THEN** the goal reports 3,500,000 IDR progress from that account
- **AND** no ledger transaction or account balance mutation is created

#### Scenario: Tracking a linked account transaction
- **WHEN** a linked account balance increases by 1,000,000 IDR through a valid account transaction
- **THEN** the goal's next response reflects the new account-backed balance without a separate goal contribution record

#### Scenario: Rejecting an invalid account link
- **WHEN** a user submits an archived, unknown, or foreign account ID for a goal
- **THEN** the entire request is rejected and the existing account links remain unchanged

#### Scenario: Linking a single child pocket
- **WHEN** a user links only a child pocket with a 3,500,000 IDR balance
- **THEN** the goal progress is exactly 3,500,000 IDR and the pocket appears once

#### Scenario: Linking parent account and child pockets simultaneously
- **WHEN** a parent with an 11,400,615 IDR aggregate and its children are linked together
- **THEN** the goal progress is 11,400,615 IDR without adding the child balances again

#### Scenario: Linking multiple sibling child pockets without parent
- **WHEN** two sibling pockets of 5,712,112 IDR and 5,688,503 IDR are linked without their parent
- **THEN** the goal progress is their 11,400,615 IDR sum

### Requirement: Goal Progress Action Presentation
The Goals interface SHALL present progress actions according to goal backing.

#### Scenario: Required behavior and constraints
- **WHEN** an owner views progress actions for a savings goal
- **THEN** the following required behavior and constraints hold:

  The Goals interface SHALL present progress actions according to goal backing:
  1. A standalone goal SHALL offer a clearly labeled progress adjustment that changes no account or ledger balance.
  2. An account-backed goal SHALL display that progress follows linked account balances and SHALL not offer a synthetic goal deposit action.
  3. Linking or unlinking accounts SHALL explain the resulting progress source before saving.
  4. The interface SHALL never label a progress-only adjustment as an account transfer or ask for a source account that will not be debited.

#### Scenario: Viewing an account-backed goal
- **WHEN** a goal has one or more linked accounts
- **THEN** the interface identifies those accounts as the progress source and omits standalone deposit controls

#### Scenario: Adjusting a standalone goal
- **WHEN** a user records progress for a goal without linked accounts
- **THEN** the confirmation states that no account transaction will be created

### Requirement: Standalone Goal Progress Adjustments
A goal without linked accounts SHALL support explicit progress adjustments that update only the goal's tracked `current_amount`. A standalone progress adjustment SHALL NOT create a ledger transaction or alter any account balance.

#### Scenario: Recording standalone progress
- **WHEN** a user adds 500,000 IDR of progress to a goal with no linked accounts
- **THEN** the goal's `current_amount` increases by 500,000 IDR
- **AND** no transaction or account balance change is created

#### Scenario: Preventing adjustment beyond valid bounds
- **WHEN** a progress adjustment would make a standalone goal negative
- **THEN** the system rejects the adjustment and preserves the existing amount
