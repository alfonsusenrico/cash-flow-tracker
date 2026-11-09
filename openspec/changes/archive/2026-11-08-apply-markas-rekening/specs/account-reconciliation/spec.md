## MODIFIED Requirements

### Requirement: Reconciliation-Required Flag on the Accounts Page
The `/accounts` page SHALL show "Perlu dicocokkan", with an icon and text rather than colour alone, on every tile whose `reconciliation_required` is true, on desktop and mobile, and again in that item's detail. On an item eligible for `Sesuaikan saldo` the label SHALL be a button that opens that item's adjustment. A Per rekening group SHALL state how many of its pockets need it.

#### Scenario: Flagged pocket opens its reconciliation
- **GIVEN** the pocket "Kantong Utama" under "Bank Jago" has `reconciliation_required = true`
- **WHEN** the owner opens `/accounts` at mobile or desktop width
- **THEN** the pocket tile SHALL show "Perlu dicocokkan" with a warning icon
- **AND** under Per rekening the "Bank Jago" group SHALL show "1 kantong perlu dicocokkan"
- **AND** activating the label with a pointer or the keyboard SHALL open "Sesuaikan saldo" for Kantong Utama without opening the tile's detail

#### Scenario: Unflagged accounts show no label
- **WHEN** an account or pocket has `reconciliation_required = false`
- **THEN** no reconciliation label SHALL be shown for it

#### Scenario: Flag on an item that cannot be reconciled here
- **GIVEN** an amount-tracked investment product with `reconciliation_required = true`
- **WHEN** the owner opens `/accounts`
- **THEN** its tile and detail SHALL show "Perlu dicocokkan" as a label without an action

#### Scenario: Flag clears after reconciliation
- **WHEN** the owner completes `Sesuaikan saldo` for a flagged account
- **THEN** the refreshed accounts list SHALL no longer show the label for that account

#### Scenario: Over limit comes first
- **WHEN** a flagged credit account is also over its limit
- **THEN** the tile SHALL show "Melewati limit" as its one note, and the detail SHALL list both
