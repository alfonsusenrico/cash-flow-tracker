# investment-amount-conversion Specification

## Purpose
Switch a unit-tracked mutual-fund product to amount tracking and turn already recorded funding expenses into reversible investment top-ups without losing ledger history.

## Requirements

### Requirement: Unit-tracked mutual fund conversion to amount tracking
The system SHALL let the owner convert an owned, active, leaf mutual-fund product that records units into an amount-tracked product, once and only by explicit request.

#### Scenario: Required behavior and constraints
- **WHEN** an owner explicitly converts a unit-tracked mutual fund to amount tracking
- **THEN** the following required behavior and constraints hold:

  The system SHALL let the owner convert an owned, active, leaf mutual-fund product that records units into an amount-tracked product, once and only by explicit request. The conversion SHALL carry over the current value as units multiplied by the last price (or the product's ledger balance when no price exists), and the invested cost as units multiplied by the average buy price, else the positive opening balance, else zero for an empty product, else unknown. After conversion the product SHALL record no units or per-unit average, SHALL be eligible for amount top-ups and amount valuation, and SHALL no longer accept unit trades.

#### Scenario: Product keeps its value and cost
- **WHEN** the owner converts a product holding 2,871.1295 units with last price 1,994.18 and average buy price 1,922.1773
- **THEN** the product reports value Rp5.725.549, invested cost Rp5.518.820, no units, and amount tracking

#### Scenario: Converted product accepts a top-up
- **WHEN** the owner records a Rp112.590 top-up into a converted product
- **THEN** the product's value and invested cost each increase by Rp112.590

#### Scenario: Ineligible product is refused
- **WHEN** the owner requests conversion of a stock, gold, parent, archived, already amount-tracked, or unit-free product, or of another user's product
- **THEN** the request is rejected with a stable error code and no account changes

### Requirement: Recorded expense conversion to an investment top-up
The system SHALL let the owner turn an owned, standalone expense from an active liquid account into an investment top-up for an owned, active, amount-tracked mutual-fund product. The request MAY carry `suggested_target_account_id`, the product a top-up suggestion offered; a newly made conversion with it SHALL store the owner's answer to the suggestion, and a request without it SHALL behave as before.

#### Scenario: Required behavior and constraints
- **WHEN** an owner converts a recorded standalone expense to an investment top-up
- **THEN** the following required behavior and constraints hold:

  The system SHALL let the owner turn an owned, standalone expense from an active liquid account into an investment top-up for an owned, active, amount-tracked mutual-fund product. The existing expense SHALL become the top-up's outgoing leg with its date, amount, notification link, and idempotency key preserved, and the system SHALL add the incoming product leg and increase the product's value and known invested cost by the amount, in one atomic operation. The funding account balance SHALL NOT change. An expense that is already part of a movement, or linked to a debt, debt split, goal, or recurring rule, SHALL be refused.

#### Scenario: Notification debit becomes a top-up
- **WHEN** the owner converts the Rp112.590 RDN expense recorded from a myBCA notification into a top-up for a Bibit product
- **THEN** the ledger shows one "Top up Investasi" from RDN BCA to that product, the RDN balance is unchanged, and the product value increases by Rp112.590

#### Scenario: Repeated request does not double count
- **WHEN** the same conversion request is submitted twice for the same expense and product
- **THEN** the second response returns the existing top-up and no further account change occurs

#### Scenario: Conflicting or ineligible expense is refused
- **WHEN** the expense is already converted to another product, is income, belongs to a movement, is linked to a debt or goal or recurring rule, or comes from a non-liquid account
- **THEN** the request is rejected with a stable error code and nothing changes

#### Scenario: Suggested product is recorded once
- **WHEN** the owner converts an expense with `suggested_target_account_id` naming another product, then repeats the same request
- **THEN** exactly one `corrected` suggestion answer SHALL be stored for that expense and the repeat SHALL store nothing

### Requirement: Converted top-up lifecycle
A top-up created from a recorded expense SHALL support the same amount, date, and notes corrections as other top-ups. Deleting it SHALL reverse its effect on the product and restore the original standalone expense with its original category, spending pillar, and notes, keeping the corrected amount and date, rather than removing the bank debit.

#### Scenario: Deleting restores the bank debit
- **WHEN** the owner deletes a top-up that was converted from an RDN expense
- **THEN** the product's value and invested cost decrease by the amount, the product leg is removed, and the original RDN expense reappears as a standalone transaction with its original category

### Requirement: Undoing an automatic top-up
Deleting a top-up recorded automatically from a learned pattern SHALL also restore the expense's review status and classifier from before the conversion, and SHALL count as the owner's rejection of that product for the debit.

#### Scenario: Deleting an automatic top-up returns the debit to Tinjau
- **WHEN** the owner deletes a top-up that was recorded automatically from a learned pattern
- **THEN** the debit SHALL again be an unlinked `needs_review` expense awaiting its counterpart, classified as before the conversion
- **AND** the deletion SHALL be stored as the owner's rejection of that product for the debit, so the debit is not converted again automatically

### Requirement: Web conversion actions
The accounts page SHALL offer "Ubah ke pelacakan nominal" in the options of an eligible unit-tracked mutual-fund product, with a confirmation that states the carried-over value and invested cost and that the change cannot be undone. The transaction edit view SHALL offer "Jadikan Top up Investasi" for an eligible expense with a product picker limited to amount-tracked mutual-fund products, and SHALL explain how to make a product eligible when none exists.

#### Scenario: Owner converts from the options menu
- **WHEN** the owner chooses "Ubah ke pelacakan nominal" on a unit-tracked Bibit product and confirms
- **THEN** the product shows Top up instead of Beli/Jual and its value is unchanged

#### Scenario: Failure stays actionable
- **WHEN** a conversion request fails
- **THEN** the dialog stays open with the error announced and the owner's selection retained

### Requirement: Conversion placement and eligibility data
The edit view SHALL place "Jadikan Top up Investasi" directly under the transaction type so that it is visible without scrolling on a 390 px wide screen. `GET /api/transactions` SHALL return each row's `recurring_rule_id`, so that the edit view does not offer the conversion for an expense the server would refuse.

#### Scenario: Conversion visible on a phone
- **WHEN** the owner opens an eligible RDN expense in the ledger edit dialog at 390 by 844 px
- **THEN** "Jadikan Top up Investasi" SHALL be inside the visible area without scrolling

#### Scenario: Recurring-linked expense is not offered
- **WHEN** the ledger lists an expense recorded by a recurring rule
- **THEN** its `recurring_rule_id` SHALL be present and the edit view SHALL NOT offer "Jadikan Top up Investasi"
