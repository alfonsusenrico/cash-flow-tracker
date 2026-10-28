# Spec Delta

## Purpose

Record rupiah contributions to mutual-fund products without requiring unit allocations, while preserving investment capital, valuation, and ledger integrity.

## ADDED Requirements

### Requirement: Amount-only investment contribution

The system SHALL accept a positive integer-rupiah contribution, funding account, target product, transaction time, optional notes, and retry identifier without requiring units or NAB.

#### Scenario: Required behavior and constraints
- **WHEN** an owner submits an amount-only investment contribution
- **THEN** the following required behavior and constraints hold:

  The system SHALL accept a positive integer-rupiah contribution, funding account, target product, transaction time, optional notes, and retry identifier without requiring units or NAB. The funding account SHALL be active, owned, and liquid. The target SHALL be an active owned mutual-fund leaf account with no unit tracking. Invalid references, insufficient funds, aggregate targets, and unit-tracked targets SHALL fail without financial changes. Existing unit-based trades SHALL remain available on unit-tracked positions, and generic transfers SHALL retain their investment eligibility protections. A product with amount-contribution history SHALL NOT be silently converted to unit tracking through an account edit or trade.

#### Scenario: Record the monthly amount
- **WHEN** the owner records Rp112.590 from BCA RDN to an eligible Bibit product
- **THEN** the system debits RDN by Rp112.590 and records the same contribution to that product without inventing units or a unit price

#### Scenario: Unsupported or foreign account
- **WHEN** a contribution names another user's account, an archived account, a parent with children, or a product tracked in units
- **THEN** the system rejects it and leaves ledger entries, units, capital, and value unchanged

#### Scenario: Insufficient funding balance
- **WHEN** the funding account has Rp100.000 and the requested contribution is Rp112.590
- **THEN** the system reports insufficient funds and creates no contribution or partial debit

### Requirement: Contributions preserve capital and gains

The system SHALL increase amount-mode cost basis and the current value estimate by the contribution amount exactly once, independently of the ledger opening balance.

#### Scenario: Required behavior and constraints
- **WHEN** an amount-mode investment contribution is recorded
- **THEN** the following required behavior and constraints hold:

  The system SHALL increase amount-mode cost basis and the current value estimate by the contribution amount exactly once, independently of the ledger opening balance. An existing explicit zero valuation SHALL remain meaningful. Unknown historical cost basis SHALL remain unknown until supplied by the owner. Contributions SHALL NOT be presented as market gains or fresh quotes. Updating actual total valuation SHALL replace the value estimate without posting another contribution or changing the opening ledger balance.

#### Scenario: Preserve existing gain
- **WHEN** a product has Rp1.000.000 cost basis and Rp1.050.000 value and receives Rp112.590
- **THEN** its basis is Rp1.112.590, estimated value is Rp1.162.590, gain stays Rp50.000, liquid assets decrease by Rp112.590, and combined net worth stays unchanged

#### Scenario: Reconcile the actual Bibit value
- **WHEN** the owner subsequently enters a total value of Rp1.170.000 for that product using Update Nilai without changing its cost basis
- **THEN** value becomes Rp1.170.000 and gain becomes Rp57.410 without a new debit or resetting contributed capital

#### Scenario: Unknown cost basis
- **WHEN** an existing amount-mode product has a value but no known historical capital and receives a contribution
- **THEN** value increases by that amount and gain stays unavailable until the owner supplies total cost basis

### Requirement: Atomic and reversible contribution lifecycle

The system SHALL commit contribution metadata, both ledger legs, and account state atomically.

#### Scenario: Required behavior and constraints
- **WHEN** an investment contribution is recorded, retried, edited or reversed
- **THEN** the following required behavior and constraints hold:

  The system SHALL commit contribution metadata, both ledger legs, and account state atomically. Repeating the same create request SHALL return the original contribution; reuse of its identifier with different financial inputs SHALL fail. Authorized edits and deletes SHALL adjust both legs, capital, and value consistently under concurrency. Generic transaction or movement operations SHALL NOT bypass these protections. Deleting a contribution originating from a recurring occurrence SHALL NOT silently make that occurrence executable again.

#### Scenario: Retry after a lost response
- **WHEN** the same request is submitted again after its first commit
- **THEN** there is one contribution and one pair of ledger entries, with no second capital or value increase

#### Scenario: Replay after deletion
- **WHEN** an original create request is retried after its contribution was deleted
- **THEN** the system reports the deleted record or a conflict without recreating the debit

#### Scenario: Correct the recorded amount
- **WHEN** the owner corrects Rp112.590 to Rp100.000
- **THEN** RDN is credited Rp12.590 and product cost basis and value each decrease Rp12.590 while existing gain is preserved

#### Scenario: Delete a contribution after a valuation update
- **WHEN** the owner deletes a Rp112.590 contribution after supplying a newer total valuation
- **THEN** the funding debit is reversed and basis and the latest value estimate each decrease Rp112.590, with the adjusted estimate requiring reconciliation against Bibit

#### Scenario: Concurrent or invalid correction
- **WHEN** concurrent corrections target one contribution or a reversal would make the investment value or known basis negative
- **THEN** corrections serialize, or the invalid operation fails atomically without losing funds or corrupting account state

### Requirement: Product entry and ledger presentation

The web interface SHALL expose Top up on eligible products with product context, funding selector, amount, date/time, notes, and a monthly-rule shortcut.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens an eligible investment product or inspects its contribution in the ledger
- **THEN** the following required behavior and constraints hold:

  The web interface SHALL expose Top up on eligible products with product context, funding selector, amount, date/time, notes, and a monthly-rule shortcut. It SHALL omit units and NAB fields and default to an eligible product funding account, then its parent's funding account. Contributions SHALL appear as one consolidated investment transfer in desktop and mobile ledgers and SHALL be excluded from ordinary income and daily expense totals. Investment-allocation summaries SHALL count the amount once. The affected controls SHALL support keyboard operation, labeled inputs, visible errors, focus restoration, and responsive layouts.

#### Scenario: Enter a top-up from a product
- **WHEN** the owner selects Top up on an amount-tracked Bibit product whose parent uses BCA RDN for funding
- **THEN** the form displays that product and defaults to BCA RDN, accepts Rp112.590 without units or NAB, and shows one Top up Investasi ledger row after success

#### Scenario: Unit-tracked product remains explicit
- **WHEN** the owner views a unit-tracked mutual-fund product
- **THEN** Beli/Jual retain their unit-based behavior and the account list omits persistent explanatory top-up eligibility text

#### Scenario: Failure remains actionable
- **WHEN** a contribution submission fails
- **THEN** the form stays open with its entered values and an understandable error, and no success state is shown
