## MODIFIED Requirements

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
