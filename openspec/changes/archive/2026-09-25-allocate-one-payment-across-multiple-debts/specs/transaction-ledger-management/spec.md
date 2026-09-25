# Spec Delta

## MODIFIED Requirements

### Requirement: Transaction Detail & Inline Editing
The application SHALL allow users to click any transaction row to open a detail and editing modal supporting:
1. Editing the transaction amount with automatic balance recalculation on affected accounts.
2. Changing the associated account or category.
3. Updating notes and transaction timestamp.
4. Linking or unlinking savings goals (`goal_id`) and debt obligations (`obligation_id`), or assigning a debt payment among multiple obligations with explicit amounts (`obligation_allocations`).
5. Showing each debt name and assigned amount for an allocated payment in ledger detail, including debts that were archived by that payment; the transaction SHALL remain one ledger row.
6. Deleting the transaction with confirmation, cleanly reversing its balance impact on accounts, goals, or obligations.

The Quick Capture and ledger edit forms SHALL let a user select multiple active debts for an expense. Selecting a new debt SHALL default its payment amount to that debt's outstanding balance and SHALL update the transaction total to the sum of selected payments. The default view SHALL show each amount without requiring a separate input. An explicit amount-edit action SHALL allow partial payments per debt and SHALL update the total as amounts change. The form SHALL prevent submission when a selected amount is not positive, exceeds the available debt balance, or does not match the transaction total. Reopening a saved payment SHALL show its recorded amounts rather than replacing them with current debt balances. The same behavior and feedback SHALL be available in desktop dialogs and mobile sheets. A single-debt selection MAY keep the existing API field for compatibility.

#### Scenario: Modifying transaction amount
- **WHEN** the user changes a transaction amount from 50,000 to 75,000 IDR and clicks Save
- **THEN** the backend updates the transaction record, reconciles the corresponding account balance by -25,000 IDR, and reflects the updated numbers across the UI

#### Scenario: Deleting a linked goal transaction
- **WHEN** the user deletes an expense linked to a savings goal
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
