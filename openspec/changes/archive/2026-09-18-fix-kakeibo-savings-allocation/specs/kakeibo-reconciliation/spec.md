# Spec Delta

## MODIFIED Requirements

### Requirement: Portfolio Trade Exclusion from Kakeibo
The system SHALL exclude investment trades (such as buying or selling stocks, mutual funds, or gold) and automated internal transfers from Kakeibo pillars so that trading turnover and inter-account movements do not inflate monthly savings or living expense metrics:
1. Category metadata for `Internal Movement` SHALL have `kakeibo_type = NULL`.
2. Investment trade transactions (manual or ingested via notification event from brokers such as Stockbit) SHALL be created with `kakeibo_type = NULL`.
3. Internal account transfer transactions (manual or ingested via notification event such as Bank Jago pocket moves) between liquid accounts SHALL be created with `kakeibo_type = NULL`.
4. Ingestion pipelines and transaction endpoints SHALL NOT default `kakeibo_type` to `"saving"` or `"need"` for trade or transfer events between liquid accounts, or for card verification pre-auth holds.

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
- **AND** the transfer SHALL NOT be counted towards Kakeibo savings if the destination is a liquid operational account

#### Scenario: Temporary card verification hold
- **WHEN** a card verification hold transaction is recorded (e.g. Rp 10.000 pre-auth hold)
- **THEN** it SHALL NOT be classified as Kakeibo `saving`

### Requirement: Net Fresh Savings Calculation
The system SHALL count only fresh net capital movements from operational accounts into savings or investment accounts as monthly savings.

#### Scenario: User transfers funds from Bank to Investment account
- **WHEN** a user transfers money from an operational checking account to an investment broker or goal account without a trade execution
- **THEN** the transfer SHALL be recognized as monthly savings
- **AND** any reverse transfer from an investment account back to an operational account SHALL be deducted from net savings

#### Scenario: User transfers funds between checking or e-wallet accounts
- **WHEN** a user transfers money from Bank ATM to GoPay or between operational checking pockets
- **THEN** the transfer amount SHALL NOT be counted as monthly savings
