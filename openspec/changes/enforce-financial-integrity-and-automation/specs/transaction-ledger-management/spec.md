# Spec Delta

## MODIFIED Requirements

### Requirement: Consolidated Internal Movement Ledger Representation
The application SHALL persist and present each bilateral internal movement as one logical operation backed by two durably linked ledger records:
1. Both records SHALL share an immutable movement identifier assigned by the server.
2. In the default "Semua Rekening" view, the linked records SHALL render as one consolidated row without timestamp, amount, note, or account-name inference.
3. The row SHALL display a neutral `↔️ PINDAH SALDO` badge, source account, destination account, and neutral tabular amount.
4. In account-filtered views, the movement SHALL display direction relative to the selected account.
5. Creating, editing, or deleting the logical movement SHALL atomically create, update, or delete both records through a server-owned movement operation.
6. An incomplete or inconsistent legacy pair SHALL remain visible as an unpaired record and SHALL NOT be combined with another movement heuristically.

#### Scenario: Viewing a durably linked movement
- **WHEN** the user views the all-accounts ledger after moving funds between two accounts
- **THEN** the two records sharing the movement identifier render as one `↔️ PINDAH SALDO` row with correct source and destination

#### Scenario: Atomically editing a movement
- **WHEN** the user changes the amount, date, source, or destination of a consolidated movement
- **THEN** the server updates both linked records in one transaction or updates neither

#### Scenario: Preserving unrelated equal-value transfers
- **WHEN** two movements have the same amount and occur within ten minutes but have different movement identifiers
- **THEN** the ledger renders them as two distinct movements

#### Scenario: Viewing internal movements in all-accounts ledger
- **WHEN** the user views the all-accounts ledger after a canonical internal movement
- **THEN** the linked pair renders as one neutral row with source, destination, and amount

#### Scenario: Deleting a consolidated internal movement
- **WHEN** the user confirms deletion of a linked movement
- **THEN** the server removes both roles atomically and restores both affected balances
