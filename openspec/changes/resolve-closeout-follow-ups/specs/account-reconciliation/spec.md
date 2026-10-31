## ADDED Requirements

### Requirement: Reconciliation-Required Flag on the Accounts Page
The `/accounts` page SHALL show a visible "Perlu rekonsiliasi" label, with an icon and text rather than colour alone, on every account card and pocket row whose `reconciliation_required` is true, on desktop and mobile. On an item eligible for `Sesuaikan Saldo` the label SHALL be a button that opens that item's reconciliation flow. A parent card SHALL state how many of its pockets need reconciliation.

#### Scenario: Flagged pocket opens its reconciliation
- **GIVEN** the pocket "Kantong Utama" under "Bank Jago" has `reconciliation_required = true`
- **WHEN** the owner opens `/accounts` at mobile or desktop width
- **THEN** the pocket row SHALL show "Perlu rekonsiliasi" with a warning icon
- **AND** the "Bank Jago" card SHALL show "1 kantong perlu rekonsiliasi"
- **AND** activating the label with a pointer or the keyboard SHALL open "Sesuaikan Saldo: Kantong Utama"

#### Scenario: Unflagged accounts show no label
- **WHEN** an account or pocket has `reconciliation_required = false`
- **THEN** no reconciliation label SHALL be shown for it

#### Scenario: Flag on an item that cannot be reconciled here
- **GIVEN** an amount-tracked investment product with `reconciliation_required = true`
- **WHEN** the owner opens `/accounts`
- **THEN** the card SHALL show "Perlu rekonsiliasi" as a label without an action

#### Scenario: Flag clears after reconciliation
- **WHEN** the owner completes `Sesuaikan Saldo` for a flagged account
- **THEN** the refreshed accounts list SHALL no longer show the label for that account
