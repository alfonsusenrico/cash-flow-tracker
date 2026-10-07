# Spec Delta

## MODIFIED Requirements

### Requirement: Consolidated Internal Movement Ledger Representation
The application SHALL persist and present each bilateral internal movement as one logical operation backed by two durably linked ledger records.

#### Scenario: Required behavior and constraints
- **WHEN** an internal movement is persisted, displayed, edited or deleted
- **THEN** the following required behavior and constraints hold:

  The application SHALL persist and present each bilateral internal movement as one logical operation backed by two durably linked ledger records:
  1. Both records SHALL share an immutable movement identifier assigned by the server.
  2. In the default "Semua Rekening" view, durably linked records SHALL render as one consolidated row regardless of their timestamp difference.
  3. The row SHALL display a neutral `↔️ PINDAH SALDO` badge, source account, destination account, and neutral tabular amount.
  4. In account-filtered views, the movement SHALL display direction relative to the selected account.
  5. Creating, editing, or deleting the logical movement SHALL atomically create, update, or delete both records through a server-owned movement operation.
  6. In the all-accounts ledger, two unlinked rows identified as internal movements SHALL render as one inferred row when they have opposite directions, equal positive amounts, different accounts, timestamps less than 30 seconds apart, and exactly one mutually eligible partner in the loaded ledger page. This match SHALL be display-only and SHALL NOT assign a movement identifier or enable atomic pair edit/delete.
  7. A linked movement SHALL never be matched heuristically with an unlinked row, and ambiguous or incomplete legacy rows SHALL remain separate.
  8. The user SHALL be able to select exactly two unlinked transactions, including transactions not categorized as internal movements, by holding an eligible ledger row for one second and then clicking rows. The ledger SHALL display a concise hold instruction and current selection count. The first hold SHALL enter selection mode and select that row; holding a uniquely inferred movement row SHALL select both underlying records together. Subsequent clicks SHALL toggle rows or inferred pairs, selection SHALL stop at two underlying records, and unselecting the final selection SHALL exit selection mode. A short click outside selection mode SHALL continue to open the record for editing. Selection SHALL survive ledger pagination and filters.
  9. The ledger SHALL NOT show a dedicated merge-entry button. The desktop ledger SHALL NOT show a row-level `Gabungkan` action or an `Aksi` column. Once exactly two records are selected, the UI SHALL show a viewport-fixed `Jadikan Pindah Saldo` action. Confirmed linked movements SHALL remain consolidated and visibly marked as `Tergabung`; uniquely inferred legacy pairs SHALL remain consolidated while selecting. Inferred and unlinked rows SHALL NOT display `perkiraan` or `Belum tertaut` labels.
  10. The `Jadikan Pindah Saldo` action SHALL fetch current transaction details and directly submit a pair with equal amounts, without an additional confirmation preview. If one income and one expense have different amounts, the UI SHALL NOT call the conversion endpoint and SHALL show a centered, dimmed warning modal with a single `Ok` button. For eligible equal-value rows, the server SHALL atomically verify same owner, one income and one expense, equal positive amounts, different eligible liquid accounts, no existing movement linkage, and no financial association that conversion would corrupt; it SHALL set the shared movement identifier, roles, and internal-movement classifications without creating or deleting transactions or changing balances. Rejected conversion SHALL leave both records unchanged.

#### Scenario: Viewing a durably linked movement
- **WHEN** the user views the all-accounts ledger after moving funds between two accounts
- **THEN** the two records sharing the movement identifier render as one `↔️ PINDAH SALDO` row with correct source and destination

#### Scenario: Atomically editing a movement
- **WHEN** the user changes the amount, date, source, or destination of a consolidated movement
- **THEN** the server updates both linked records in one transaction or updates neither

#### Scenario: Preserving unrelated equal-value transfers
- **WHEN** two movements have the same amount and occur within ten minutes but have different movement identifiers
- **THEN** the ledger renders them as two distinct movements

#### Scenario: Inferring a unique legacy pair
- **WHEN** two unlinked internal-movement rows are each other's only eligible opposite-direction match and their timestamps differ by 29 seconds
- **THEN** the all-accounts ledger shows one inferred movement row without persisting linkage or offering paired edit/delete

#### Scenario: Keeping an ambiguous or late pair separate
- **WHEN** unlinked rows are exactly 30 seconds apart or one row has multiple eligible partners
- **THEN** the ledger keeps the rows separate for explicit selection

#### Scenario: Selecting transactions by holding a ledger row
- **WHEN** the user holds an eligible row for one second
- **THEN** the ledger enters selection mode and highlights that row, subsequent row clicks add or remove selections up to two, and removing the last selection exits selection mode

#### Scenario: Editing with a short click
- **WHEN** the user clicks a row without entering selection mode
- **THEN** the existing edit flow opens for that record

#### Scenario: Keeping inferred pairs consolidated during selection
- **WHEN** the user enters selection mode while the all-accounts ledger contains a uniquely inferred legacy pair
- **THEN** the pair remains one `Pindah Saldo` row without a `perkiraan` or `Belum tertaut` label

#### Scenario: Selecting an inferred pair as a unit
- **WHEN** the user holds a uniquely inferred legacy movement row for one second
- **THEN** both underlying transaction records are selected together and the ledger continues to show one row

#### Scenario: Keeping durable movement status during selection
- **WHEN** the user enters selection mode while the ledger contains a durably linked movement
- **THEN** that movement remains one consolidated row visibly marked `Tergabung`

#### Scenario: Converting two equal-value transactions directly
- **WHEN** exactly one eligible expense and one eligible income from different accounts are selected and their current amounts match
- **THEN** the floating `Jadikan Pindah Saldo` action submits the conversion without a confirmation preview, both existing records receive one shared movement identifier and internal-movement classification atomically, and their balances and original timestamps remain unchanged

#### Scenario: Rejecting unequal transaction amounts before conversion
- **WHEN** the selected expense and income have different amounts
- **THEN** the conversion endpoint is not called and a centered dim-background warning modal displays a single `Ok` button

#### Scenario: Confirming two uncategorized transactions as a movement
- **WHEN** the user selects one eligible expense and one eligible income from different accounts and activates the floating conversion action
- **THEN** both existing records receive one shared movement identifier and internal-movement classification atomically, their balances and original timestamps remain unchanged, and subsequent ledger views show one durable movement

#### Scenario: Rejecting an unsafe manual conversion
- **WHEN** one selected row is already linked, has a debt allocation, or the amounts differ
- **THEN** the server rejects the operation and neither row changes

#### Scenario: Viewing internal movements in all-accounts ledger
- **WHEN** the user views the all-accounts ledger after a canonical internal movement
- **THEN** the linked pair renders as one neutral row with source, destination, and amount

#### Scenario: Deleting a consolidated internal movement
- **WHEN** the user confirms deletion of a linked movement
- **THEN** the server removes both roles atomically and restores both affected balances
