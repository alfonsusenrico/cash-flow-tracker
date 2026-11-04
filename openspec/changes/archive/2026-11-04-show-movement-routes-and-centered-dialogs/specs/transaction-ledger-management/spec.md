## MODIFIED Requirements

### Requirement: Transaction Detail & Inline Editing
The application SHALL let users open any transaction row in a detail drawer (a centered modal dialog above 760 px, a bottom sheet at 760 px and below) that shows the row's provenance and supports editing, a type switch, confirming and deleting. The drawer SHALL NOT open as a side column or a side overlay, and SHALL NOT narrow the page behind it.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens transaction details or edits a ledger payment
- **THEN** the following required behavior and constraints hold:

  The application SHALL allow users to select any transaction row to open a detail drawer supporting:
  1. Editing the transaction amount with automatic balance recalculation on affected accounts.
  2. Changing the associated account or category.
  3. Updating notes and transaction timestamp.
  4. Linking or unlinking savings goals (`goal_id`) and debt obligations (`obligation_id`), or assigning a debt payment among multiple obligations with explicit amounts (`obligation_allocations`).
  5. Showing each debt name and assigned amount for an allocated payment in ledger detail, including debts that were archived by that payment; the transaction SHALL remain one ledger row.
  6. Deleting the transaction with confirmation, cleanly reversing its balance impact on accounts, goals, or obligations.
  7. Showing where the row came from ("Dari HP", "Lewat Catat", "Dicatat manual" and the other sources), who classified it ("AI", "Kebiasaan", "Pengguna", "Aturan"), and the original notification or typed text when the row has one.
  8. Opening the existing movement or top-up edit dialog for a movement or top-up row.
  9. Titling a movement row with its route in the direction the money moved, with the same "Dari" and "Ke" accounts as the route; an unlinked leg's side is taken from its type and its missing side is shown as "?".

  The Quick Capture and ledger edit forms SHALL let a user select multiple active debts for an expense. Selecting a new debt SHALL default its payment amount to that debt's outstanding balance and SHALL update the transaction total to the sum of selected payments. The default view SHALL show each amount without requiring a separate input. An explicit amount-edit action SHALL allow partial payments per debt and SHALL update the total as amounts change. The form SHALL prevent submission when a selected amount is not positive, exceeds the available debt balance, or does not match the transaction total. Reopening a saved payment SHALL show its recorded amounts rather than replacing them with current debt balances. The same behavior and feedback SHALL be available in desktop drawers and mobile sheets. A single-debt selection MAY keep the existing API field for compatibility.

#### Scenario: Modifying transaction amount
- **WHEN** the user changes a transaction amount from 50,000 to 75,000 IDR and clicks Save
- **THEN** the backend updates the transaction record, reconciles the corresponding account balance by -25,000 IDR, and reflects the updated numbers across the UI

#### Scenario: Deleting a linked goal transaction
- **WHEN** the user deletes an expense linked to a savings goal and confirms
- **THEN** the transaction is removed and the goal's `current_amount` decreases by the transaction amount

#### Scenario: Recording one payment for multiple debts
- **WHEN** a user selects two debts for a single expense and allocates the full expense amount between them
- **THEN** Quick Capture shows each assigned amount before submission and the ledger subsequently shows one transaction with the two-debt breakdown

#### Scenario: Selecting debts defaults to full payoff
- **WHEN** a user selects debts with 80,000 IDR and 50,000 IDR outstanding
- **THEN** the form shows those payment amounts without extra amount inputs and sets the transaction total to 130,000 IDR

#### Scenario: Changing to a partial payment
- **WHEN** the user activates the amount-edit action and reduces the first selected debt payment from 80,000 IDR to 30,000 IDR
- **THEN** the transaction total updates to 80,000 IDR while the second selected debt remains at 50,000 IDR

#### Scenario: Correcting an incomplete allocation
- **WHEN** selected debt amounts do not add up to the expense amount
- **THEN** the form shows the remaining or excess amount, focuses an actionable field or summary, and sends no request

#### Scenario: Editing an allocated payment
- **WHEN** a user opens an existing allocated expense in the ledger and changes the debt amounts
- **THEN** the saved breakdown appears unchanged until the user edits it, the transaction total follows the edited amounts, and the updated transaction still appears as one ledger row with the new breakdown

#### Scenario: Delete asks first
- **WHEN** the owner presses "Hapus" in the drawer
- **THEN** nothing SHALL be deleted until the owner confirms in a message that names the amount and the balance, goal or debt it changes; "Batal" SHALL keep the row

#### Scenario: Deleting a top-up row
- **WHEN** the owner deletes a top-up converted from a debit and confirms
- **THEN** the drawer SHALL use the top-up delete path, the inbound leg SHALL be gone and the original expense SHALL be restored

#### Scenario: Waiting row points to Tinjau
- **WHEN** the owner opens a row with an open question or an unpaired movement leg
- **THEN** the drawer SHALL show "Jawab di Tinjau" linking to that item and SHALL NOT offer "Tandai sudah dicek"

#### Scenario: Drawer on a phone
- **WHEN** the owner opens a row at 390 px width
- **THEN** the drawer SHALL be a bottom sheet with focus inside it, and the browser's back SHALL close it and return focus to the row

#### Scenario: Detail opens centered on a wide window
- **WHEN** the owner opens a row at 1440 px or 1024 px width
- **THEN** the detail SHALL open as a centered dialog over a dimmed page, with focus inside it and the edit form at its full width, and no side column SHALL appear
- **AND** Esc, the close button or a click on the dimmed page SHALL close it and return focus to the row

#### Scenario: Inbound leg titled in its direction
- **GIVEN** an inbound leg on Jago · Main Pocket linked to an outbound leg on Jago · Dana Darurat
- **WHEN** the owner opens the inbound leg from Rekening detail of Main Pocket
- **THEN** the title SHALL read "Jago · Dana Darurat → Jago · Main Pocket", "Dari" SHALL be Jago · Dana Darurat and "Ke" Jago · Main Pocket

### Requirement: Consolidated Internal Movement Ledger Representation
The application SHALL persist and present each bilateral internal movement as one logical operation backed by two durably linked ledger records.

#### Scenario: Required behavior and constraints
- **WHEN** an internal movement is persisted, displayed, edited or deleted
- **THEN** the following required behavior and constraints hold:

  The application SHALL persist and present each bilateral internal movement as one logical operation backed by two durably linked ledger records:
  1. Both records SHALL share an immutable movement identifier assigned by the server.
  2. In the default "Semua Rekening" view, durably linked records SHALL render as one consolidated row regardless of their timestamp difference.
  3. The row SHALL display a neutral `↔️ PINDAH SALDO` badge, its route as `Sumber → Tujuan` in the direction the money moved, and the amount in neutral tabular typography without misleading `+` or `-` prefixes, reflecting zero net cash flow change. A pocket SHALL be named with its parent account, as `Jago · Main Pocket`; an account without a parent SHALL be named alone.
  4. In account-filtered views, the movement SHALL display its full route and its direction relative to the selected account, as "Masuk dari" or "Keluar ke" the other account. A leg shown without its partner SHALL take its route from its own movement role and the partner's account, never reversed.
  4a. A movement leg without a partner, such as an unlinked row filed as Internal Movement, SHALL show its own account on the side its type implies (target for an income, source for an expense), "?" for the other side and the label "Pasangan belum ditemukan". The missing side SHALL NOT be guessed from another row.
  5. Creating, editing, or deleting the logical movement SHALL atomically create, update, or delete both records through a server-owned movement operation.
  6. In the all-accounts ledger, two unlinked rows identified as internal movements SHALL render as one inferred row when they have opposite directions, equal positive amounts, different accounts, timestamps less than 30 seconds apart, and exactly one mutually eligible partner in the loaded ledger page. This match SHALL be display-only and SHALL NOT assign a movement identifier or enable atomic pair edit/delete.
  7. A linked movement SHALL never be matched heuristically with an unlinked row, and ambiguous or incomplete legacy rows SHALL remain separate.
  8. The user SHALL be able to select exactly two unlinked transactions, including transactions not categorized as internal movements, by holding an eligible ledger row for one second and then clicking rows. The ledger SHALL display a concise hold instruction and current selection count. The first hold SHALL enter selection mode and select that row; holding a uniquely inferred movement row SHALL select both underlying records together. Subsequent clicks SHALL toggle rows or inferred pairs, selection SHALL stop at two underlying records, and unselecting the final selection SHALL exit selection mode. A short click outside selection mode SHALL continue to open the record for editing. Selection SHALL survive ledger pagination and filters.
  9. The ledger SHALL NOT show a dedicated merge-entry button. The desktop ledger SHALL NOT show a row-level `Gabungkan` action or an `Aksi` column. Once exactly two records are selected, the UI SHALL show a viewport-fixed `Jadikan Pindah Saldo` action. Confirmed linked movements SHALL remain consolidated and visibly marked as `Tergabung`; uniquely inferred legacy pairs SHALL remain consolidated while selecting. Inferred and unlinked rows SHALL NOT display `perkiraan` or `Belum tertaut` labels.
  10. The `Jadikan Pindah Saldo` action SHALL fetch current transaction details and directly submit a pair with equal amounts, without an additional confirmation preview. If one income and one expense have different amounts, the UI SHALL NOT call the conversion endpoint and SHALL show a centered, dimmed warning modal with a single `Ok` button. For eligible equal-value rows, the server SHALL atomically verify same owner, one income and one expense, equal positive amounts, different eligible liquid accounts, no existing movement linkage, and no plan, goal, transfer-fee or other financial association that conversion would corrupt; it SHALL set the shared movement identifier, roles, and internal-movement classifications without creating or deleting transactions or changing balances. Rejected conversion SHALL leave both records unchanged.

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

#### Scenario: Route names the parent account
- **WHEN** the all-accounts ledger shows a linked movement from the Jago pocket Main Pocket to the BCA pocket ATM
- **THEN** the row SHALL read "Jago · Main Pocket → BCA · ATM"

#### Scenario: Inbound leg in an account-filtered list
- **GIVEN** a movement of Rp 400.000 from Jago · Dana Darurat to Jago · Main Pocket
- **WHEN** the ledger is filtered to Main Pocket
- **THEN** the row SHALL read "Jago · Dana Darurat → Jago · Main Pocket" with "Masuk dari Jago · Dana Darurat", and SHALL NOT read "Jago · Main Pocket → Jago · Dana Darurat"

#### Scenario: Unlinked movement income
- **GIVEN** an unlinked income of Rp 500.000 on Jago · Main Pocket filed as Internal Movement
- **WHEN** the ledger or Rekening detail lists it
- **THEN** the row SHALL read "? → Jago · Main Pocket" with "Pasangan belum ditemukan"

#### Scenario: Transaction rows carry parent names
- **WHEN** the client requests `GET /api/transactions` or a transaction's detail
- **THEN** each row SHALL include `account_parent_name` and `transfer_target_account_parent_name`, `null` for an account without a parent or a leg without a partner, and every existing field SHALL keep its value
