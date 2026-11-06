## MODIFIED Requirements

### Requirement: Scheduled Amount
"Terjadwal" SHALL be the unpaid occurrences of the owner's active non-income plans that are due or overdue up to the day before the expected salary date, plus the unfunded part of the current period's goal set-asides. Paid, linked or skipped occurrences SHALL NOT count, and a set-aside already moved into the goal's accounts this period SHALL NOT count again. A transfer plan into a credit account SHALL count only up to that account's opening debt left to pay.

#### Scenario: Linked payment leaves Terjadwal
- **GIVEN** a Rp 600.000 plan due on 5 November inside the window
- **WHEN** its payment is linked to the plan
- **THEN** "Terjadwal" SHALL fall by Rp 600.000 while "Keluar" rises by the payment, so the remaining amount does not count it twice

#### Scenario: Goal set-aside already funded
- **GIVEN** a goal with a Rp 600.000 set-aside for the period and Rp 400.000 already moved into its pocket this period
- **WHEN** the scheduled amount is computed
- **THEN** the goal SHALL contribute Rp 200.000 and its explanation line SHALL show both amounts

#### Scenario: Without an expected salary date
- **WHEN** the expected salary date is unknown
- **THEN** "Terjadwal" SHALL cover the next 30 days, and its explanation SHALL name that window

#### Scenario: Credit-bill plan is not counted twice
- **GIVEN** a Rp 470.000 SPayLater purchase this cycle, no opening debt, and a Rp 470.000 transfer plan Jago → SPayLater due in the window
- **WHEN** the scheduled amount is computed
- **THEN** the plan's line SHALL show `plan_amount` Rp 470.000, `amount` Rp 0 and `counted_elsewhere` Rp 470.000, and "Aman dipakai" SHALL be lower by Rp 470.000 only, through Pengeluaran

#### Scenario: Credit-bill plan for opening debt
- **GIVEN** Kredivo opened owing Rp 300.000 with nothing paid, and two Rp 200.000 transfer plan occurrences into Kredivo in the window
- **WHEN** the scheduled amount is computed
- **THEN** the two occurrences SHALL count Rp 200.000 and Rp 100.000 in due-date order, and the rest SHALL be marked as counted elsewhere

## ADDED Requirements

### Requirement: Credit Figures in the Cycle Summary
The cycle summary SHALL return `spending.credit` with two figures. `spent_on_credit` is the period's reportable expenses on credit accounts, a part of `spending.amount`. `paid_to_credit` is the cash moved into credit accounts in the period, from accounts that are not credit. Each SHALL have per-account lines with `label_source` `user`. Neither SHALL change Pengeluaran, Keluar, Masuk, Ditabung & investasi or Aman dipakai.

#### Scenario: Purchase and payment in one cycle
- **GIVEN** Pengeluaran of Rp 1.000.000 including a Rp 120.000 SPayLater dinner, and a Rp 470.000 movement Jago → SPayLater in the cycle
- **WHEN** the cycle summary is read
- **THEN** `spent_on_credit` SHALL be Rp 120.000, `paid_to_credit` Rp 470.000, and Pengeluaran SHALL stay Rp 1.000.000

#### Scenario: No credit accounts
- **WHEN** an owner has no credit account
- **THEN** both figures SHALL be 0 with no lines, and every other figure SHALL equal its value before this change

#### Scenario: Payment from a savings account
- **GIVEN** a credit account and a role-`tabungan` pocket
- **WHEN** the owner moves Rp 200.000 from that pocket into the credit account
- **THEN** `paid_to_credit` SHALL grow by Rp 200.000, and "Ditabung & investasi" SHALL fall by Rp 200.000, as for any money leaving savings
