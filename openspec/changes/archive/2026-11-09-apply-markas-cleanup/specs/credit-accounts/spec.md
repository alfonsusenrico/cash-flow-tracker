## MODIFIED Requirements

### Requirement: Credit Account Overview
`GET /api/credit/accounts/{id}` SHALL return the account's owed amount, limit, limit left (which may be negative), overpayment, days, next due bill, its bills with their items, and totals for a period: spending on credit, fees, refunds, cash paid to it, and items settled and settled on time. The period SHALL be given as local days (`from`, `to`) or as the ledger list's own window (`period` with `period_offset`, or instants `from_date`, `to_date`), resolved exactly as `GET /api/transactions` resolves it. `GET /api/credit/bills` SHALL list bills across the owner's credit accounts. Credit rows of `GET /api/accounts` SHALL gain `statement_day`, `limit_left` and `next_due`.

#### Scenario: Overview totals
- **GIVEN** this cycle: Rp 200.000 spent on SPayLater, a Rp 10.000 fee, and Rp 480.000 paid from Jago
- **WHEN** the owner reads SPayLater's overview for the cycle
- **THEN** the totals SHALL be spending on credit Rp 210.000, fees Rp 10.000 and cash paid Rp 480.000

#### Scenario: The ledger list's window
- **GIVEN** a cycle that started with the salary received at 08.02 on 27 October, SPayLater spending of Rp 459.000 so far, a Rp 20.000 purchase at 07.30 on 27 October and a Rp 77.000 purchase timed 23.00 today while it is 12.39
- **WHEN** the owner reads SPayLater's overview with `period=cycle&period_offset=0`
- **THEN** spending on credit SHALL be Rp 459.000, the same as the `type=credit` list for that account and window, and the overview with `from=2026-10-27&to=<today>` SHALL still count whole local days

#### Scenario: Two period forms at once
- **WHEN** the overview is asked with `period=cycle` and `from=2026-11-01`, or with `period=month` and `to_date`
- **THEN** it SHALL be refused with 422 `period_with_dates`

#### Scenario: Bills due soon across accounts
- **WHEN** the owner lists bills due by 13 December
- **THEN** SPayLater's 11 December bill and Kredivo's 1 December bill SHALL be listed with their account names and left to pay

#### Scenario: Not a credit account
- **WHEN** the overview is asked for a bank account, or for another owner's credit account
- **THEN** it SHALL be refused with 422 `credit_account_required`, or answered 404

#### Scenario: Error in the account's language
- **WHEN** an English account sends terms on a bank expense
- **THEN** the 422 SHALL carry `code` `credit_terms_not_credit_purchase` and an English `message`, and the code SHALL exist in both server catalogues

### Requirement: Due Dates Kept After a Day Change
Changing a credit account's `payment_day` or `statement_day` SHALL keep the due date of every purchase whose first due date was on or before today, and SHALL move only bills not yet due. Changing a purchase's date SHALL clear a kept due date. It SHALL also clear an owner due date that falls before the new date, and the response SHALL say so. Changing only a purchase's instalment count SHALL keep its kept due date as kept, not as the owner's.

#### Scenario: Payment day moves from 11 to 5
- **GIVEN** a purchase due and paid on 11 November and a purchase due on 11 December, with today 20 November
- **WHEN** the owner changes the payment day to 5
- **THEN** the first SHALL stay due on 11 November and paid on time, and the second SHALL be due on 5 December

#### Scenario: Purchase date moved past the owner's due date
- **WHEN** the owner moves a purchase with an owner first due date of 1 December to 3 December
- **THEN** the purchase SHALL get its derived due date, and the response SHALL carry `credit_terms_reset: true`

#### Scenario: Count changed on a purchase with a kept due date
- **GIVEN** the first purchase above, kept due on 11 November after the payment day moved to 5
- **WHEN** the owner changes only its instalments from 1 to 3 and saves
- **THEN** `PUT /api/transactions/{id}/credit-terms` SHALL carry `first_due_date` 11 November, the stored terms SHALL keep 11 November with `first_due_date_set_by` `system`, and its instalments SHALL fall on 11 November, 11 December and 11 January
