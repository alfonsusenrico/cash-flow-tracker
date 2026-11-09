## MODIFIED Requirements

### Requirement: 3-Second Rapid Transaction Capture Pad
The application SHALL provide a prominent capture entry: "+ Catat" in the workspace top bar, the Catat button of the phone tab bar, and the `N` key while single-key shortcuts are on. The manual form SHALL offer Pengeluaran, Pemasukan and Perpindahan, a 56 px amount field, quick-add chips, and categories grouped by Kakeibo pillar.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens and completes rapid transaction capture
- **THEN** it SHALL provide a category selector grouped by pillar, default to the primary spend account, support inline arithmetic, and refresh observed figures and activity without a full page reload

#### Scenario: Organized category selection in quick capture
- **WHEN** a user opens the manual form and opens "Kategori"
- **THEN** the expense categories SHALL be listed under their pillar names, and the chosen category SHALL set the pillar unless the owner chose another pillar

#### Scenario: Desktop keyboard shortcut capture
- **WHEN** single-key shortcuts are on and a user presses `N` on any screen outside a text field
- **THEN** the capture dialog opens with the sentence field focused
- **AND** when single-key shortcuts are off, pressing `N` does nothing

#### Scenario: Amount focus
- **WHEN** the amount field receives focus, in either theme
- **THEN** its edge SHALL be 1 px `--field-focus` with a 3 px `--ring`, and no outline, coloured or 2 px border SHALL appear

### Requirement: Quick Capture Linking to Goals and Obligations
The global capture form SHALL allow optional direct linking of an expense to one savings goal or to one or more debt obligations, inside "Detail tambahan", each with a one-line hint. A goal and a debt SHALL be exclusive, and the debt choice SHALL not be offered for an expense on a credit account.

#### Scenario: Required behavior and constraints
- **WHEN** an owner links a capture expense to a goal or debt
- **THEN** the following required behavior and constraints hold:

  The global capture form SHALL allow optional direct linking to savings goals and debt obligations:
  1. When recording an expense, the user can select an active Goal (marking it as a milestone contribution) or one or more active Obligations (marking a debt repayment, split by amount).
  2. Upon submission, the transaction SHALL record the corresponding `goal_id`, `obligation_id` or `obligation_allocations` and automatically update the goal's progress or decrement each obligation's `remaining_amount` in the database.

#### Scenario: Contributing to a goal via quick capture
- **WHEN** the user presses `N`, chooses "Isi manual", enters `500000`, selects account "Main Bank", opens "Detail tambahan" and chooses goal "Emergency Fund"
- **THEN** an expense of 500,000 IDR is recorded and linked to "Emergency Fund", and the goal's saved amount increases by 500,000 IDR immediately

#### Scenario: Details open when needed
- **WHEN** the form holds a goal passed by its caller or an error inside "Detail tambahan"
- **THEN** "Detail tambahan" SHALL be open with that field visible

## ADDED Requirements

### Requirement: Inline Arithmetic in the Amount
The capture amount field SHALL accept digits and the operators `+ − * /` typed on any keyboard, evaluate the expression without `eval`, show "= {amount}" as its hint only while an operator is present, and save the evaluated whole-rupiah amount. No on-screen keypad SHALL be shown.

#### Scenario: Evaluate inline arithmetic in transaction entry
- **WHEN** the owner types `25000+15000` in "Nominal (IDR)"
- **THEN** the hint SHALL read "= Rp 40.000" and saving SHALL send `amount` 40000

#### Scenario: Plain amount
- **WHEN** the owner types `35000`
- **THEN** the field SHALL show "35.000" and no "=" hint
