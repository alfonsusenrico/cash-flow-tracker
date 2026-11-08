# account-reconciliation Specification

## Purpose
Allow users to reconcile tracked account balances with an actual balance and retain an auditable adjustment.

## Requirements

### Requirement: 1-Tap Account Balance Reconciliation
The application SHALL provide a `Sesuaikan Saldo` balance reconciliation action for every eligible account or pocket in `/accounts`, reachable from that item's labelled actions.

#### Scenario: Reconciling an account balance discrepancy
- **WHEN** the user inputs an actual balance of 10,000,000 IDR for an account currently tracking at 9,950,000 IDR
- **THEN** the backend updates the account balance to 10,000,000 IDR and creates an income adjustment transaction of 50,000 IDR dated today
- **AND** the adjustment SHALL belong to the income category "Penyesuaian Saldo".

#### Scenario: Reconciling negative balance discrepancy
- **WHEN** the user inputs an actual balance of 9,900,000 IDR for an account currently tracking at 9,950,000 IDR
- **THEN** the backend updates the account balance to 9,900,000 IDR and creates an expense adjustment transaction of 50,000 IDR labeled "Balance Adjustment"
- **AND** the adjustment SHALL belong to the expense category "Penyesuaian Saldo".

#### Scenario: Adjustments do not distort reports
- **GIVEN** an expense adjustment of 50,000 IDR in the current cycle
- **WHEN** the dashboard computes spending, Kakeibo allocation and safe-to-spend
- **THEN** the adjustment SHALL NOT be counted in any of them
- **AND** the account balance SHALL still reflect it.

#### Scenario: Existing uncategorised adjustments are migrated
- **GIVEN** a stored transaction with notes "Balance Adjustment (Reconciliation)", no category, no movement and no idempotency key
- **WHEN** the schema migration runs
- **THEN** the transaction SHALL move to the "Penyesuaian Saldo" category of its kind
- **AND** transactions with any other notes or an existing category SHALL NOT change.

#### Scenario: Required behavior and constraints
- **WHEN** an owner reconciles the recorded balance of an eligible account or pocket
- **THEN** the following required behavior and constraints hold:

  The application SHALL provide a `Sesuaikan Saldo` balance reconciliation action for every eligible account or pocket in `/accounts`, reachable from that item's labelled actions:
  1. The modal SHALL display the current tracked balance and provide an input for the user's actual real-world balance.
  2. The system SHALL display the calculated variance (e.g. `+15,000 IDR` or `-8,500 IDR`).
  3. Upon confirmation, the backend SHALL update the account balance to the actual amount and automatically generate an audit transaction labeled "Balance Adjustment" or custom user notes.

#### Scenario: Finding reconciliation in the redesigned hierarchy
- **WHEN** a user opens the actions for an eligible account or pocket on desktop or mobile
- **THEN** a clearly labelled `Sesuaikan Saldo` action opens the existing reconciliation flow for that exact item

### Requirement: Reconciliation Variance Preview
The reconciliation modal SHALL display the current tracked balance, provide an input for the user's actual real-world balance, and display the calculated variance (e.g. `+15,000 IDR` or `-8,500 IDR`).

#### Scenario: Variance shown before confirmation
- **GIVEN** an account tracking 9,950,000 IDR
- **WHEN** the user types an actual balance of 9,965,000 IDR in the reconciliation modal
- **THEN** the modal SHALL show the tracked balance and a variance of `+15,000 IDR` before anything is saved.

### Requirement: Reconciliation Audit Transaction
On confirmation, the backend SHALL update the account balance to the actual amount and generate an audit transaction labelled "Balance Adjustment" or with the user's custom notes.

#### Scenario: Custom notes label the adjustment
- **WHEN** the user confirms a reconciliation with the notes "Selisih bunga"
- **THEN** the account balance SHALL equal the actual amount
- **AND** the audit transaction SHALL carry the notes "Selisih bunga".

### Requirement: Penyesuaian Saldo System Category
The audit transaction SHALL use the user's balance-adjustment system category (key `balance_adjustment`, seeded name "Penyesuaian Saldo") of the matching kind (income or expense), whatever its current name. That category SHALL be excluded from budget, Kakeibo, spending and income reports, and SHALL carry no Kakeibo pillar. The system SHALL create the category when no category of that kind carries the key.

#### Scenario: Missing adjustment category is created
- **GIVEN** a user without an income "Penyesuaian Saldo" category
- **WHEN** the user reconciles an account upwards
- **THEN** the system SHALL create the income category "Penyesuaian Saldo" with key `balance_adjustment`, excluded from budget and without a pillar, and file the adjustment under it.

#### Scenario: Renamed adjustment category is reused
- **GIVEN** the user renamed the expense adjustment category to "Koreksi saldo"
- **WHEN** the user reconciles an account downwards
- **THEN** the adjustment SHALL be filed under "Koreksi saldo", and no new "Penyesuaian Saldo" category SHALL be created.

### Requirement: Recategorising an Adjustment
The system SHALL let the user recategorise an adjustment to an ordinary category when it represents real spending or income.

#### Scenario: Adjustment that was real spending
- **GIVEN** an expense adjustment of 50,000 IDR in "Penyesuaian Saldo"
- **WHEN** the user moves it to "Makanan & Minuman"
- **THEN** it SHALL count as spending in that category like any other expense.

### Requirement: Reconciliation-Required Account State
`GET /api/accounts` SHALL expose for each account whether it requires reconciliation (`reconciliation_required`), the reason, the notification event that caused it and the tracked discrepancy, without exposing notification payload text. Completing a successful balance reconciliation SHALL clear the state in the same database transaction as the balance-adjustment transaction.

#### Scenario: Clearing a notification discrepancy
- **WHEN** a user reconciles an account that was flagged after a settled notification exceeded its tracked balance
- **THEN** the adjustment transaction commits and the reconciliation-required state is cleared together

### Requirement: Reconciliation-Required Flag on the Accounts Page
The `/accounts` page SHALL show a visible "Perlu rekonsiliasi" label, with an icon and text rather than colour alone, on every account row and pocket row whose `reconciliation_required` is true, on desktop and mobile, and again in that item's drawer. On an item eligible for `Sesuaikan Saldo` the label SHALL be a button that opens that item's reconciliation flow. A parent row SHALL state how many of its pockets need reconciliation.

#### Scenario: Flagged pocket opens its reconciliation
- **GIVEN** the pocket "Kantong Utama" under "Bank Jago" has `reconciliation_required = true`
- **WHEN** the owner opens `/accounts` at mobile or desktop width
- **THEN** the pocket row SHALL show "Perlu rekonsiliasi" with a warning icon
- **AND** the "Bank Jago" row SHALL show "1 kantong perlu rekonsiliasi"
- **AND** activating the label with a pointer or the keyboard SHALL open "Sesuaikan Saldo: Kantong Utama"

#### Scenario: Unflagged accounts show no label
- **WHEN** an account or pocket has `reconciliation_required = false`
- **THEN** no reconciliation label SHALL be shown for it

#### Scenario: Flag on an item that cannot be reconciled here
- **GIVEN** an amount-tracked investment product with `reconciliation_required = true`
- **WHEN** the owner opens `/accounts`
- **THEN** its row and drawer SHALL show "Perlu rekonsiliasi" as a label without an action

#### Scenario: Flag clears after reconciliation
- **WHEN** the owner completes `Sesuaikan Saldo` for a flagged account
- **THEN** the refreshed accounts list SHALL no longer show the label for that account

### Requirement: Manual Balance Check
The web app SHALL offer "Cocokkan saldo" for every liquid account, pocket, and parent whose pockets are all liquid. It runs only when the owner asks, never on a schedule. It SHALL compare the typed balance with the recorded balance without changing the ledger. A parent SHALL be compared as its own rows plus its pockets' rows, each counted once.

#### Scenario: Exact match
- **GIVEN** a pocket recorded at Rp 1.250.000
- **WHEN** the owner checks it with Rp 1.250.000
- **THEN** the difference SHALL be Rp 0, no row SHALL be written, and "Saldo cocok" SHALL be offered

#### Scenario: Parent checked as a group
- **GIVEN** a parent with Rp 100.000 of its own and two pockets with Rp 400.000 and Rp 500.000
- **WHEN** the owner checks the parent with Rp 1.000.000
- **THEN** the recorded balance SHALL be Rp 1.000.000, and "Sesuaikan saldo" SHALL NOT be offered for the parent

#### Scenario: Not a liquid account
- **WHEN** the owner checks an investment product or a parent holding one
- **THEN** the check SHALL be refused with `balance_check_not_liquid`, and another owner's account SHALL answer 404

### Requirement: Balance Check Causes
The check SHALL list likely causes found deterministically, with no AI, in the window since the account's last matched point or adjustment, or the last 30 days without one:
- a possible double booking;
- an unpaired movement leg;
- a held, skipped or unrecorded report;
- a report still being processed.

Each cause SHALL link to its row or Tinjau item. When at most three causes sum exactly to the difference, the check SHALL name them as the explanation.

#### Scenario: Difference explained by a double booking
- **GIVEN** a pocket with a Rp 500.000 income from a Jago notification, and a Rp 500.000 GoPay income on another account two seconds apart
- **WHEN** the owner checks the pocket with a balance Rp 500.000 lower than recorded
- **THEN** the explanation SHALL be that income, as a possible double booking, linked to its row

#### Scenario: Difference explained by an unpaired leg
- **GIVEN** a Rp 75.000 row filed as Internal Movement without a partner
- **WHEN** the check's difference is −Rp 75.000
- **THEN** the explanation SHALL be that unpaired leg

#### Scenario: Unexplained difference
- **WHEN** no set of at most three causes sums to the difference
- **THEN** the check SHALL show the difference and the causes without an explanation line

### Requirement: Matched Balance Point
"Saldo cocok" SHALL record a matched point only when the typed balance equals the recorded balance at that moment, and SHALL otherwise be refused with `balance_mismatch` and the difference. The next check of that account SHALL start from its newest matched point.

#### Scenario: Next check starts at the match
- **GIVEN** a matched point recorded on 3 November
- **WHEN** the owner checks the account again on 5 November
- **THEN** only rows and reports after the matched point SHALL be considered, and the basis SHALL read "Sejak saldo cocok"

### Requirement: Balance Check With Adjustment
"Sesuaikan saldo" SHALL keep its request, response and adjustment row unchanged. When it is opened from the balance check and succeeds, the web app SHALL record a matched point at the adjusted balance.

#### Scenario: Adjusting from the check
- **WHEN** the owner checks a pocket, sees a difference of +Rp 20.000 and chooses "Sesuaikan saldo"
- **THEN** one "Penyesuaian Saldo" income of Rp 20.000 SHALL be recorded as before, followed by a matched point at the typed balance
