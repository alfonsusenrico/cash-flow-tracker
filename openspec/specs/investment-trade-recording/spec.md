# investment-trade-recording Specification

## Purpose
Record instrument trades with unit and cost inputs and preview their effect on investment positions.

## Requirements

### Requirement: Investment Trade Transaction Creation

The system SHALL accept investment trade parameters (`investment_action`, `units`, `price_per_unit`) when creating a transaction and execute position mutation alongside ledger transfer entry. It SHALL reject unit-based trades against a product already using amount tracking with contribution history, rather than implicitly converting that product or replacing its contributed capital.

#### Scenario: Successfully record a stock purchase with position accumulation
- **WHEN** user records a buy trade for 6 lots of BBRI at Rp3.200 funded by RDN BCA
- **THEN** system records a transfer from RDN BCA to BBRI for Rp 1.920.000, increments BBRI units by 600, and recalculates the weighted average purchase price

#### Scenario: Successfully record an investment sale
- **WHEN** user records a sell trade for 400 units of BBRI to RDN BCA at Rp3.500
- **THEN** system records a transfer from BBRI to RDN BCA for Rp 1.400.000 and decrements BBRI units by 400

#### Scenario: Prevent implicit conversion of an amount portfolio
- **WHEN** a unit-based buy or sell targets an amount-tracked mutual-fund product with contribution history
- **THEN** the system rejects it with an explanation and leaves units, cost basis, valuation, and ledger balances unchanged

### Requirement: Dynamic Multi-Instrument Trade Input
The user interface SHALL render instrument-specific input fields for stocks, mutual funds, gold, and crypto, computing the total transaction amount dynamically.

#### Scenario: Trading gold in grams
- **WHEN** user selects a Gold investment account in the trade modal
- **THEN** modal shows weight in grams and price per gram, auto-calculating total IDR

#### Scenario: Trading mutual funds in UP
- **WHEN** user selects a Mutual Fund investment account in the trade modal
- **THEN** modal shows Unit Penyertaan (UP) and NAB per unit, auto-calculating total IDR

### Requirement: Real-Time Position Impact Preview
The user interface SHALL display a live preview comparing current units and average buy price with the projected post-trade units and average buy price prior to submission.

#### Scenario: Averaging down preview
- **WHEN** user inputs 6 lots at Rp3.200 for a stock currently holding 1,400 shares at Rp3.340
- **THEN** UI displays projected position: 2,000 shares with new average price Rp 3.298

### Requirement: Trade Settlement Form Clarity
The trade form SHALL identify the instrument position and funding account, show the direction of cash and units for the selected action, validate positive units and price, and prevent submission of a sale above the currently owned position. The projected position and cash settlement SHALL remain visible before confirmation.

#### Scenario: Previewing a sale destination
- **WHEN** a user prepares to sell an investment into RDN BCA
- **THEN** the form shows units leaving the instrument, proceeds entering RDN BCA, and the projected remaining units before submission

#### Scenario: Blocking an excessive sale in the form
- **WHEN** requested sale units exceed the displayed owned units
- **THEN** the submit action is blocked and the owned-versus-requested error is associated with the units control
