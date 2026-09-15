## ADDED Requirements

### Requirement: Hierarchical Accounts and Child Pockets
The system SHALL support organizing accounts into a clean two-level hierarchy (Parent Account -> Child Pockets / Sub-accounts) using a self-referential `parent_id`:
1. An account with `parent_id IS NULL` that has no children SHALL act as a standalone account (e.g. Cash, GoPay).
2. An account with `parent_id IS NULL` that has children SHALL act as a Master/Parent account (e.g. Bank Jago, BCA, Bibit).
3. An account with `parent_id IS NOT NULL` SHALL act as a child pocket belonging to that parent account.
4. Child pockets SHALL NOT have sub-pockets (enforcing a strict two-level depth).
5. Deleting or archiving a parent account SHALL cascade cleanly to its child pockets.

#### Scenario: Creating a child pocket under a bank account
- **WHEN** an authenticated user creates an account "Kantong Liburan" with `parent_id` pointing to "Bank Jago"
- **THEN** the system persists "Kantong Liburan" as a child pocket of "Bank Jago"

#### Scenario: Enforcing two-level hierarchy limit
- **WHEN** a user attempts to create a child account whose parent is already a child pocket
- **THEN** the system rejects the creation with a 400 Bad Request error

### Requirement: Parent Balance Aggregation and Grouped Representation
The system SHALL compute the balance of any master account dynamically as the sum of all its active child pockets' balances plus any direct balance of the master account:
1. `GET /api/accounts` SHALL return accounts with `parent_id`, `is_parent`, `children: [...]`, and aggregated total balance for master accounts.
2. Transactions recorded against a child pocket SHALL directly debit or credit that specific pocket and automatically reflect in the master account's aggregated balance.
3. Account selectors across Quick Capture, Ledger, and Transfers SHALL present pockets grouped under their respective master accounts using `<optgroup>`.
4. The Accounts page SHALL render master accounts with expandable/collapsible pocket lists and clear visual hierarchy.

#### Scenario: Querying accounts with pockets
- **WHEN** a user with Bank Jago having "Kantong Utama" (Rp 5.000.000) and "Kantong Darurat" (Rp 15.000.000) calls `GET /api/accounts`
- **THEN** Bank Jago reports a total aggregated balance of Rp 20.000.000 with both child pockets nested under it

#### Scenario: Recording expense from a specific pocket
- **WHEN** a user records an expense of Rp 50.000 from "Kantong Utama"
- **THEN** "Kantong Utama" balance decreases by Rp 50.000 and the parent "Bank Jago" aggregated balance decreases by Rp 50.000

### Requirement: Inter-Pocket Transfers
The system SHALL support atomic transfers between pockets belonging to the same parent account or across different parent accounts using the standard transfer mechanism without affecting cashflow spending metrics.

#### Scenario: Transferring money between pockets of the same bank
- **WHEN** a user transfers Rp 1.000.000 from "Kantong Utama" to "Kantong Darurat"
- **THEN** "Kantong Utama" decreases by Rp 1.000.000, "Kantong Darurat" increases by Rp 1.000.000, and Bank Jago total balance remains unchanged
