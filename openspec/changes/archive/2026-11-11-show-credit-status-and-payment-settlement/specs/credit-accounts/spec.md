## ADDED Requirements

### Requirement: Unpaid Credit Purchase Colour
A credit purchase that is not fully settled SHALL show its amount in the credit colour (`--credit-fg`) wherever a transaction row amount is shown, with its minus sign and its "pay later" text. Once fully settled it SHALL show in the expense colour with its "paid {date}" text and SHALL remain a credit purchase. No total, balance or bill SHALL change.

#### Scenario: Unpaid purchase
- **GIVEN** an unpaid Rp 120.000 purchase on SPayLater
- **WHEN** the owner reads Transactions in either theme
- **THEN** its amount SHALL read "−Rp 120.000" in `--credit-fg` and its row SHALL say "pay later"

#### Scenario: Partly paid instalments
- **GIVEN** a 6-instalment purchase with 2 instalments settled
- **WHEN** the row is shown
- **THEN** its amount SHALL stay in `--credit-fg`

#### Scenario: Fully paid
- **WHEN** a purchase's last item is settled on 4 December
- **THEN** its amount SHALL be in the expense colour, its row SHALL say "paid 4 Dec", and it SHALL still appear under the Credit filter

#### Scenario: Ordinary spending
- **WHEN** a bank expense is shown
- **THEN** its amount SHALL be in the expense colour

#### Scenario: Figures unchanged
- **WHEN** the colour is applied
- **THEN** Spending, safe to spend, balances and bills SHALL equal their values before this change

#### Scenario: Readable and distinct
- **WHEN** `--credit-fg` is measured in either theme
- **THEN** it SHALL have at least 4.5:1 contrast on canvas, surface and surface-2, SHALL be at least 14 OKLab units from `--out-fg`, and SHALL be its own token, separate from `--warn-fg`

### Requirement: Payment Lists What It Paid
The detail of a payment into a credit account SHALL list the items that payment settled, in settlement order, with the share it paid of each, and the part kept for later. The list SHALL be computed from the account's schedule and SHALL NOT be stored. The shares plus the part kept for later SHALL equal the payment.

#### Scenario: One payment clears a bill
- **GIVEN** SPayLater purchases of Rp 1.000.000, Rp 500.000 and Rp 1.002.500 and instalment 3 of 6 at Rp 100.000, all due on one bill
- **WHEN** the owner opens the Rp 2.602.500 payment
- **THEN** the detail SHALL list those four items with their full amounts, `kept_for_later` SHALL be 0, and instalments 4 to 6 SHALL NOT be listed

#### Scenario: Two payments share a purchase
- **GIVEN** one Rp 300.000 purchase paid by Rp 100.000 and then Rp 200.000
- **WHEN** the owner opens the first payment
- **THEN** it SHALL list the purchase with `amount` 100000 and `item_amount` 300000

#### Scenario: Payment above what is owed
- **WHEN** a Rp 500.000 payment covers Rp 470.000 of items
- **THEN** `kept_for_later` SHALL be 30000

#### Scenario: Opened from either side
- **WHEN** the detail is requested for either leg of the payment movement
- **THEN** both SHALL return the same list

#### Scenario: Purchase deleted after payment
- **WHEN** a listed purchase is deleted
- **THEN** the payment's list SHALL be recomputed without it and the freed amount SHALL settle the next items or be kept for later

#### Scenario: Account without a payment day
- **WHEN** the payment's credit account has no payment day
- **THEN** the list SHALL be empty and `due_dates_unavailable` SHALL be true

#### Scenario: Foreign payment
- **WHEN** an owner requests another owner's payment
- **THEN** the response SHALL be 404 and SHALL reveal no items

#### Scenario: Rows and the list endpoint
- **WHEN** the ledger list is requested
- **THEN** its payment rows SHALL NOT carry `settled_items`
