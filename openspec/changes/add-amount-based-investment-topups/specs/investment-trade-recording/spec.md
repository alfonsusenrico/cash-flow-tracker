# Spec Delta

## MODIFIED Requirements

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
