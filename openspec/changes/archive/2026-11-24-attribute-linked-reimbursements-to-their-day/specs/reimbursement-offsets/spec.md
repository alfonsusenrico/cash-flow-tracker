## MODIFIED Requirements

### Requirement: Offset Attribution to Spending Days
An amount linked to an expense SHALL be subtracted on that expense's own day, in its category and effective pillar. A category's other applied offset (unlinked reimbursements and the rest of linked ones) SHALL be attributed to the days that category was spent on in the window, in proportion to what each day has left, so that no day is negative and the days sum to the window's applied offset.

#### Scenario: Single spending day
- **GIVEN** Rp 90.000 spent in "Makan Bareng" on one day of the window and an applied offset of Rp 30.000 for that category
- **WHEN** 30-day analytics are requested
- **THEN** that day SHALL show Rp 60.000 and no day SHALL be negative.

#### Scenario: A repaid meal stays on its day
- **GIVEN** a Rp 205.350 meal in "Makanan & Minuman" on 31 October with two Rp 68.450 reimbursements linked to it, and Rp 88.290 of "Makanan & Minuman" on 23 November
- **WHEN** the cycle's analytics are requested
- **THEN** 31 October SHALL count the meal as Rp 68.450, 23 November SHALL show Rp 88.290 for that category, and the cycle's Pengeluaran SHALL be the same as before this change

#### Scenario: An unlinked reimbursement still spreads
- **GIVEN** Rp 60.000 and Rp 240.000 spent in "Makan Bareng" on two days and a Rp 150.000 reimbursement naming the category without links
- **WHEN** 30-day analytics are requested
- **THEN** the two days SHALL show Rp 30.000 and Rp 120.000
