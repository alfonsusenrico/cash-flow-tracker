# kakeibo-reconciliation Specification

## Purpose
TBD - created by archiving change portfolio-trade-kakeibo-reconciliation. Update Purpose after archive.
## Requirements
### Requirement: Portfolio Trade Exclusion from Kakeibo
The system SHALL exclude investment trades (such as buying or selling stocks, mutual funds, or gold) and automated internal transfers from Kakeibo pillars so that trading turnover and inter-account movements do not inflate monthly savings or living expense metrics:
1. Investment trade transactions (manual or ingested via notification event from brokers such as Stockbit) SHALL be created with `kakeibo_type = NULL`.
2. Internal account transfer transactions (manual or ingested via notification event such as Bank Jago pocket moves) SHALL be created with `kakeibo_type = NULL`.
3. Ingestion pipelines SHALL NOT default `kakeibo_type` to `"saving"` or `"need"` for trade or transfer events.

#### Scenario: User records a stock purchase trade
- **WHEN** a user records an investment trade to buy or sell units of a stock or mutual fund
- **THEN** the resulting transaction SHALL have `kakeibo_type = NULL`
- **AND** the transaction amount SHALL NOT be added to the Kakeibo "Tabungan" pillar

#### Scenario: Ingesting Stockbit trade notification
- **WHEN** a push notification event from Stockbit indicates an order execution (e.g. *"Pembelian 10 lot BBRI match di harga Rp3.340"*)
- **THEN** the system SHALL create a ledger transfer transaction with `kakeibo_type = NULL`
- **AND** the trade amount SHALL NOT appear in Kakeibo expense or savings pillars

#### Scenario: Ingesting internal transfer notification
- **WHEN** a push notification indicates an internal transfer between user accounts or pockets
- **THEN** the system SHALL create a ledger transfer transaction with `kakeibo_type = NULL`

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

