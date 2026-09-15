# investment-trade-recording Specification

## Purpose
TBD - created by archiving change investment-trade-transactions. Update Purpose after archive.
## Requirements
### Requirement: Investment Trade Transaction Creation
The system SHALL accept investment trade parameters (`investment_action`, `units`, `price_per_unit`) when creating a transaction and execute position mutation alongside ledger transfer entry.

#### Scenario: Successfully record a stock purchase with position accumulation
- **WHEN** user records a buy trade for 6 lots of BBRI at Rp3.200 funded by RDN BCA
- **THEN** system records a transfer from RDN BCA to BBRI for Rp 1.920.000, increments BBRI units by 600, and recalculates the weighted average purchase price

#### Scenario: Successfully record an investment sale
- **WHEN** user records a sell trade for 400 units of BBRI to RDN BCA at Rp3.500
- **THEN** system records a transfer from BBRI to RDN BCA for Rp 1.400.000 and decrements BBRI units by 400

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

