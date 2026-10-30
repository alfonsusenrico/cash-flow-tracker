# investment-trade-recording Specification

## Purpose
Record instrument trades with unit and cost inputs and preview their effect on investment positions.

## Requirements

### Requirement: Investment Trade Transaction Creation
The system SHALL accept `investment_action`, positive `units`, and positive `price_per_unit` and SHALL settle the cash and position effects atomically.

#### Scenario: Required behavior and constraints
- **WHEN** an investment trade is submitted for recording
- **THEN** the following required behavior and constraints hold:

  The system SHALL accept `investment_action`, positive `units`, and positive `price_per_unit` and SHALL settle the cash and position effects atomically:
  1. A purchase SHALL debit the selected funding account, credit the investment movement destination, increment units, and recalculate weighted average purchase price.
  2. A sale SHALL decrement the instrument units and credit the selected funding account with the proceeds.
  3. The funding and instrument accounts SHALL be active, user-owned, distinct, and compatible with the requested trade.
  4. A purchase SHALL obey liquid source balance protection.
  5. A sale SHALL be rejected when requested units exceed owned units; units SHALL never be silently clamped.
  6. Invalid, missing, zero, or non-finite trade quantities or prices SHALL be rejected without creating a ledger record.
  7. Unit-based trades against a product already using amount tracking with contribution history SHALL be rejected rather than implicitly converting that product or replacing its contributed capital.

#### Scenario: Rejecting an oversell
- **WHEN** a user owning 300 units attempts to sell 400 units
- **THEN** the system rejects the trade and leaves both cash and position unchanged

#### Scenario: Successfully record a stock purchase with position accumulation
- **WHEN** a user records 6 lots of BBRI at 3,200 IDR funded by RDN BCA
- **THEN** the system settles 1,920,000 IDR from RDN BCA, adds 600 units, and recalculates weighted average cost atomically

#### Scenario: Successfully record an investment sale
- **WHEN** a user records a sale of 400 owned BBRI units at 3,500 IDR to RDN BCA
- **THEN** the system credits 1,400,000 IDR to RDN BCA and decrements the position by 400 atomically

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
