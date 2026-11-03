## MODIFIED Requirements

### Requirement: Goal Milestone Progress & Monthly Pacing
The system SHALL compute the funding progress percentage and required monthly contribution pace for individual goals.

#### Scenario: Required behavior and constraints
- **WHEN** the system calculates a goal milestone and contribution pace
- **THEN** the following required behavior and constraints hold:

  The system SHALL compute the funding progress percentage and required monthly contribution pace for individual goals:
  1. `percentage_completed`: `min(100, round((current_amount / target_amount) * 100))`.
  2. `monthly_target_pace`: `max(0, round((target_amount - current_amount) / max(1, remaining_months)))`.
  3. The Target & Tagihan page SHALL show each goal as its own row in "Target tabungan" with its name, a labelled progress bar (the bar hidden from assistive technology, the percentage and amounts in text), the saved and target amounts, its linked accounts or "Dicatat manual", its monthly pace when it has a target date, and a status pill in words. It SHALL NOT display an aggregated global target summary that sums unrelated goals together. Activating a row SHALL open the goal's drawer with its existing actions.

#### Scenario: Viewing goals view without misleading aggregate banner
- **WHEN** the user navigates to the Goals page with an Emergency Fund goal and a Long-term Savings goal
- **THEN** both goals render as individual rows with their own progress, without an overarching combined target sum

#### Scenario: Goal without a target date
- **WHEN** a goal has no `target_date`
- **THEN** its row SHALL show the saved and target amounts and the percentage, and SHALL show no monthly pace instead of an invented one

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

#### Scenario: Editing an account-backed goal
- **WHEN** the owner changes the name, target or date of a goal with linked accounts in the edit form
- **THEN** the change SHALL be saved, the request SHALL carry no `current_amount`, and progress SHALL keep following the linked balances
- **AND** a request that sends `current_amount` for an account-backed goal SHALL be refused with 409
