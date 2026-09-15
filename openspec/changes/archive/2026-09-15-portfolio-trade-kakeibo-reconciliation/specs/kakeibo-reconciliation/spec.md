# Capability: Kakeibo Reconciliation

## ADDED Requirements

### Requirement: Portfolio Trade Exclusion from Kakeibo
The system SHALL exclude investment trades (such as buying or selling stocks, mutual funds, or gold) from Kakeibo pillars so that trading turnover does not inflate monthly savings.

#### Scenario: User records a stock purchase trade
- **WHEN** a user records an investment trade to buy or sell units of a stock or mutual fund
- **THEN** the resulting transaction SHALL have `kakeibo_type = NULL`
- **AND** the transaction amount SHALL NOT be added to the Kakeibo "Tabungan" pillar

### Requirement: Net Fresh Savings Calculation
The system SHALL count only fresh net capital movements from operational accounts into savings or investment accounts as monthly savings.

#### Scenario: User transfers funds from Bank to Investment account
- **WHEN** a user transfers money from an operational checking account to an investment broker or goal account without a trade execution
- **THEN** the transfer SHALL be recognized as monthly savings
- **AND** any reverse transfer from an investment account back to an operational account SHALL be deducted from net savings

### Requirement: Normalized 50/30/20 Allocation Proportions
The system SHALL compute Kakeibo pillar percentages based on the total allocated living funds (`total_allocated = need_spent + want_spent + max(0, net_saving_spent)`) so that pillar proportions represent a cohesive 100% distribution.

#### Scenario: Allocation percentages always sum to 100%
- **WHEN** the dashboard calculates the 50/30/20 Kakeibo breakdown for the cycle
- **THEN** `need_pct`, `want_pct`, and `saving_pct` SHALL be computed relative to `total_allocated`
- **AND** the three percentages SHALL sum to 100% (within rounding)
- **AND** no individual pillar percentage SHALL exceed 100% of the active allocation
