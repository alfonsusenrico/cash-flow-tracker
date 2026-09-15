## ADDED Requirements

### Requirement: Account-Backed Goals (Single or Multiple Linked Accounts)
The system SHALL allow users to link a savings goal to one or more physical accounts or pockets (such as money market funds, gold savings, deposits, or dedicated bank accounts):
1. A goal MAY be associated with zero, one, or multiple active accounts via `account_ids`.
2. When a goal has linked accounts, the goal's `current_amount` SHALL dynamically equal the sum of the current balances of all its linked accounts.
3. The Goal creation and edit modals SHALL provide a multi-account selector displaying account names, types, and current balances, and SHALL calculate the combined balance in real time as accounts are selected.
4. When any linked account's balance changes (via income, expense, transfer, or balance reconciliation), any goal linked to that account SHALL automatically reflect the updated balance on subsequent queries.
5. If a goal is created without linked accounts, it SHALL allow entering or tracking progress via standalone manual contributions.
6. The Goal presentation view SHALL display the list of linked accounts and their respective balances associated with the goal.

#### Scenario: Creating an Emergency Fund backed by multiple accounts
- **WHEN** a user creates a goal "Dana Darurat" with a target of 20,000,000 IDR and links it to "RDPU Bibit" (5,000,000 IDR), "Tabungan Emas" (5,000,000 IDR), and "Deposito" (5,000,000 IDR)
- **THEN** the goal is created with `current_amount` of 15,000,000 IDR (75% completed) without requiring manual entry of a starting balance

#### Scenario: Real-time goal progress update upon account transfer
- **WHEN** a user transfers 2,000,000 IDR from their checking account into a linked "RDPU Bibit" account
- **THEN** the goal "Dana Darurat" automatically reflects an updated `current_amount` of 17,000,000 IDR (85% completed)

#### Scenario: Standalone unlinked goal
- **WHEN** a user creates a goal without linking any physical accounts
- **THEN** the goal operates as a virtual savings target where progress is updated via manual contributions or explicit goal transfers
