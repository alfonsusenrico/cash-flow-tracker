# Spec Delta

## ADDED Requirements

### Requirement: Standalone Goal Progress Adjustments
A goal without linked accounts SHALL support explicit progress adjustments that update only the goal's tracked `current_amount`. A standalone progress adjustment SHALL NOT create a ledger transaction or alter any account balance.

#### Scenario: Recording standalone progress
- **WHEN** a user adds 500,000 IDR of progress to a goal with no linked accounts
- **THEN** the goal's `current_amount` increases by 500,000 IDR
- **AND** no transaction or account balance change is created

#### Scenario: Preventing adjustment beyond valid bounds
- **WHEN** a progress adjustment would make a standalone goal negative
- **THEN** the system rejects the adjustment and preserves the existing amount

## MODIFIED Requirements

### Requirement: Account-Backed Goals (Single or Multiple Linked Accounts)
The system SHALL allow users to link a savings goal to one or more active physical accounts or pockets:
1. A goal MAY be associated with zero, one, or multiple active user-owned accounts via `account_ids`; unknown, archived, or foreign accounts SHALL be rejected rather than ignored.
2. When a goal has linked accounts, its displayed progress SHALL dynamically equal the sum of the non-redundant current balances of those accounts.
3. Linking or unlinking accounts SHALL NOT create ledger transactions, copy balances into standalone progress, or otherwise change account balances.
4. If both a parent and any children are linked, the parent's aggregate balance SHALL be counted once and child balances SHALL NOT be added again.
5. If only children are linked, their balances SHALL be summed normally.
6. Direct standalone progress adjustments SHALL be unavailable while accounts are linked.
7. Every subsequent income, expense, movement, trade, or reconciliation affecting a linked account SHALL be reflected in goal progress on the next query without a goal-specific transaction.
8. The goal view SHALL display contributing accounts without redundant child chips.

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

## REMOVED Requirements

### Requirement: Linking Ledger Transactions to Goals
**Reason**: Goal progress is now either a standalone non-ledger adjustment or a derived linked-account balance; transaction-driven mutation of `goals.current_amount` creates duplicate or synthetic cash flow.

**Migration**: Existing transactions retain their historical `goal_id` for display, but future transactions do not mutate standalone progress and linked goals derive progress exclusively from account balances.
