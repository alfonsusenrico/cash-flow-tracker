## ADDED Requirements

### Requirement: Reimbursements Linked to Expenses
A reimbursement SHALL be able to link to 1 to 20 of the owner's expenses. Its amount SHALL be allocated to them by expense date then id, each taking at most what it cost less what earlier reimbursements already hold on it. A linked amount SHALL lower its expense's category and effective pillar in the period of the expense's date, whatever the reimbursement's date, and a reimbursement with links SHALL ignore its category link.

#### Scenario: Repaid in the next cycle
- **GIVEN** three CG Fun expenses of Rp 60.600, Rp 193.555 and Rp 138.225 on 28–29 October in Makanan & Minuman, Budaya, and a cycle that starts on 30 October
- **WHEN** four Rp 75.000 reimbursements of 31 October link to all three
- **THEN** the previous cycle's Pengeluaran, Makanan & Minuman and Budaya SHALL fall by Rp 300.000, allocated 60.600, 193.555 and 45.845 by date, and the current cycle's spending SHALL not change

#### Scenario: Only the expense it repaid
- **GIVEN** a Rp 25.500 Belanja expense with the Tak Terduga pillar and other Belanja expenses in the same cycle
- **WHEN** a reimbursement links to that expense alone
- **THEN** Tak Terduga SHALL fall by Rp 25.500 and no other Belanja expense or pillar SHALL change

#### Scenario: Two reimbursements on one expense
- **GIVEN** a Rp 205.350 expense and two Rp 68.450 reimbursements dated 21.38 and 21.40
- **WHEN** both link to it
- **THEN** each SHALL hold Rp 68.450 and the expense SHALL count Rp 68.450 in Pengeluaran

#### Scenario: Never below zero
- **WHEN** reimbursements linked to an Rp 80.000 expense add up to Rp 120.000
- **THEN** the expense SHALL count Rp 0, the later reimbursement SHALL hold only what was left, and its rest SHALL be unallocated

#### Scenario: Not the owner's or not an expense
- **WHEN** a link names another owner's transaction, an income, a movement leg, or more than 20 expenses, or the reimbursement's category does not offset spending
- **THEN** the API SHALL answer 422 with a code in both catalogues and change nothing

### Requirement: The Rest of a Linked Reimbursement
When linking, the owner SHALL choose what happens to the amount beyond what the links hold: not counted, which is the default and reports it as unallocated, or recorded as income in an income category the owner picks. Recording it SHALL lower the reimbursement to what its links hold and write a separate income row with the rest on the same account, date and notes, so no balance changes.

#### Scenario: A gift on top of a repayment
- **GIVEN** a Rp 100.000 reimbursement and a Rp 25.500 expense
- **WHEN** the owner links them and records the rest as income in "Pendapatan Lain"
- **THEN** the reimbursement SHALL be Rp 25.500, a Rp 74.500 income in "Pendapatan Lain" SHALL exist on the same account and date, that period's Masuk SHALL rise by Rp 74.500, and the account balance SHALL be unchanged

#### Scenario: Rest not counted
- **WHEN** the owner keeps the default
- **THEN** the reimbursement SHALL keep its amount and the rest SHALL be reported as `reimbursement_unallocated`

### Requirement: Links Follow Changes
Editing the amount, date, type or category of a linked expense or reimbursement, or deleting either, SHALL recompute the allocations of every reimbursement sharing the expenses involved. An expense that is no longer an expense, or a reimbursement whose category no longer offsets spending, SHALL lose its links.

#### Scenario: Expense lowered
- **GIVEN** a Rp 80.000 expense with a Rp 40.000 reimbursement linked
- **WHEN** the expense is edited to Rp 30.000
- **THEN** the link SHALL hold Rp 30.000 and Rp 10.000 SHALL be unallocated

#### Scenario: Reimbursement deleted
- **GIVEN** two reimbursements linked to one expense, the second holding less than its amount
- **WHEN** the first is deleted
- **THEN** the second SHALL be recomputed and hold up to its full amount

### Requirement: Linking in the Web App
The reimbursement's edit dialog and Catat SHALL offer "Mengganti transaksi" for an income in an offsetting category: a search over the owner's expenses of the last 90 days showing title, date and amount, choosing several. After saving, the dialog SHALL show each link's allocated amount and the rest with its choice. An expense's transaction dialog SHALL show "Diganti Rp X" with each reimbursement.

#### Scenario: Linking from the edit dialog
- **WHEN** the owner opens a Rp 100.000 reimbursement, picks "beli kopi titipan mama" Rp 25.500, chooses "Dicatat sebagai uang masuk" with "Pendapatan Lain" and saves
- **THEN** `PUT /api/transactions/{id}/reimburses` SHALL be sent with the expense and the rest's category, and the dialog SHALL show Rp 25.500 linked and Rp 74.500 recorded as income

#### Scenario: Seeing it on the expense
- **WHEN** the owner opens "beli kopi titipan mama"
- **THEN** its dialog SHALL read "Diganti Rp 25.500" with the reimbursement's date and note

#### Scenario: Keyboard and screen reader
- **WHEN** the picker is used by keyboard
- **THEN** the search, each result and the chosen list SHALL be reachable and named, and the choice SHALL be announced
