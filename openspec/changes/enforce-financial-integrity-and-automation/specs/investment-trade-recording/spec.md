# Spec Delta

## MODIFIED Requirements

### Requirement: Investment Trade Transaction Creation
The system SHALL accept `investment_action`, positive `units`, and positive `price_per_unit` and SHALL settle the cash and position effects atomically:
1. A purchase SHALL debit the selected funding account, credit the investment movement destination, increment units, and recalculate weighted average purchase price.
2. A sale SHALL decrement the instrument units and credit the selected funding account with the proceeds.
3. The funding and instrument accounts SHALL be active, user-owned, distinct, and compatible with the requested trade.
4. A purchase SHALL obey liquid source balance protection.
5. A sale SHALL be rejected when requested units exceed owned units; units SHALL never be silently clamped.
6. Invalid, missing, zero, or non-finite trade quantities or prices SHALL be rejected without creating a ledger record.

#### Scenario: Successfully recording a stock purchase
- **WHEN** a user buys 600 BBRI units at 3,200 IDR using RDN BCA
- **THEN** 1,920,000 IDR is atomically settled from RDN BCA to the BBRI position, units increase by 600, and weighted average price is recalculated

#### Scenario: Successfully settling an investment sale
- **WHEN** a user sells 400 owned BBRI units at 3,500 IDR to RDN BCA
- **THEN** BBRI units decrease by 400 and RDN BCA receives 1,400,000 IDR in the same operation

#### Scenario: Rejecting an oversell
- **WHEN** a user owning 300 units attempts to sell 400 units
- **THEN** the system rejects the trade and leaves both cash and position unchanged

#### Scenario: Successfully record a stock purchase with position accumulation
- **WHEN** a user records 6 lots of BBRI at 3,200 IDR funded by RDN BCA
- **THEN** the system settles 1,920,000 IDR from RDN BCA, adds 600 units, and recalculates weighted average cost atomically

#### Scenario: Successfully record an investment sale
- **WHEN** a user records a sale of 400 owned BBRI units at 3,500 IDR to RDN BCA
- **THEN** the system credits 1,400,000 IDR to RDN BCA and decrements the position by 400 atomically
