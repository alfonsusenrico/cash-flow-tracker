# kakeibo-reconciliation Specification

## Purpose
Reconcile Kakeibo spending and fresh savings while excluding portfolio trades from inappropriate allocations.

## Requirements

### Requirement: Portfolio Trade Exclusion from Kakeibo
The system SHALL exclude investment trades (such as buying or selling stocks, mutual funds, or gold) and automated internal transfers from Kakeibo pillars so that trading turnover and inter-account movements do not inflate monthly savings or living expense metrics.

#### Scenario: Required behavior and constraints
- **WHEN** investment trades or internal transfers are classified for Kakeibo reporting
- **THEN** the following required behavior and constraints hold:

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

### Requirement: Normalized 50/30/20 Allocation Proportions
The system SHALL compute Kakeibo pillar amounts for `need`, `want`, `culture`, `unexpected` and `saving` (saving is net fresh saving, never below zero) and SHALL express each pillar as a percentage of `base = max(total_inflow, total_allocated)`, where `total_allocated = need_spent + want_spent + culture_spent + unexpected_spent + max(0, net_saving_spent)`. The percentages therefore never exceed 100% in total.

#### Scenario: Allocation percentages never exceed 100%
- **WHEN** the dashboard calculates the Kakeibo breakdown for the cycle
- **THEN** `need_pct`, `want_pct`, `culture_pct`, `unexpected_pct` and `saving_pct` SHALL be computed relative to `base`
- **AND** the five percentages SHALL sum to at most 100% (within rounding).

#### Scenario: Allocation percentages always sum to 100%
- **GIVEN** a cycle whose `total_allocated` is at least its `total_inflow`
- **WHEN** the dashboard calculates the Kakeibo breakdown
- **THEN** `need_pct`, `want_pct`, `culture_pct`, `unexpected_pct` and `saving_pct` SHALL sum to 100% (within rounding).

#### Scenario: Unallocated inflow stays unallocated
- **GIVEN** a cycle with inflow Rp 2.000.000, need Rp 500.000 and no other allocation
- **WHEN** the Kakeibo breakdown is calculated
- **THEN** `need_pct` SHALL be 25.0
- **AND** the remaining 75% SHALL NOT be attributed to any pillar.

#### Scenario: Lifestyle group over target
- **GIVEN** a cycle with inflow Rp 1.000.000, need Rp 400.000, want Rp 250.000, culture Rp 150.000, unexpected Rp 0 and saving Rp 200.000
- **WHEN** the Kakeibo breakdown is calculated
- **THEN** `lifestyle_pct` SHALL be 40.0
- **AND** `kakeibo_status` SHALL be `warning`.

#### Scenario: Essentials group over target
- **GIVEN** a cycle with inflow Rp 1.000.000, need Rp 450.000, unexpected Rp 150.000, want Rp 100.000, culture Rp 0 and saving Rp 300.000
- **WHEN** the Kakeibo breakdown is calculated
- **THEN** `essentials_pct` SHALL be 60.0 and `lifestyle_pct` SHALL be 10.0
- **AND** `kakeibo_status` SHALL be `elevated_needs`.

#### Scenario: Healthy allocation is shown as healthy
- **GIVEN** a breakdown with `kakeibo_status = healthy`
- **WHEN** the owner opens the Kakeibo section in Insights
- **THEN** the status badge SHALL show the healthy label
- **AND** it SHALL show the warning label only for `warning` or `elevated_needs`.

#### Scenario: Empty cycle
- **GIVEN** a cycle with no inflow and no allocated spending
- **WHEN** the Kakeibo breakdown is calculated
- **THEN** every pillar and group percentage SHALL be 0.0
- **AND** `kakeibo_status` SHALL be `healthy`.

### Requirement: 50/30/20 Group Benchmark
The system SHALL judge the 50/30/20 benchmark on three groups: essentials = need + unexpected, target at most 50%; lifestyle = want + culture, target at most 30%; saving, target at least 20%.

#### Scenario: Group percentages
- **GIVEN** a cycle with inflow Rp 1.000.000, need Rp 300.000, unexpected Rp 100.000, want Rp 150.000, culture Rp 50.000 and saving Rp 400.000
- **WHEN** the Kakeibo breakdown is calculated
- **THEN** `essentials_pct` SHALL be 40.0 and `lifestyle_pct` SHALL be 20.0.

### Requirement: Kakeibo Status
The returned `kakeibo_status` SHALL be `warning` when lifestyle exceeds 30%; otherwise `elevated_needs` when essentials exceed 50%; otherwise `healthy`.

#### Scenario: Lifestyle warning takes precedence
- **GIVEN** a cycle whose lifestyle group is 35% and whose essentials group is 55%
- **WHEN** the Kakeibo breakdown is calculated
- **THEN** `kakeibo_status` SHALL be `warning`.

### Requirement: Stable Kakeibo Response Fields
Existing Kakeibo breakdown response fields SHALL keep their meaning.

#### Scenario: Existing fields keep their meaning
- **WHEN** a client that predates the five-pillar breakdown reads `need_pct`, `want_pct`, `saving_pct` and `kakeibo_status`
- **THEN** each field SHALL still be present and carry the meaning defined by this capability.
