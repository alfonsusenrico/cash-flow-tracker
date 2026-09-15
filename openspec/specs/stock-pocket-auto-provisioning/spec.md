# stock-pocket-auto-provisioning Specification

## Purpose
TBD - created by archiving change stock-pocket-auto-provisioning. Update Purpose after archive.
## Requirements
### Requirement: Structured Stock Trade Notification Parsing
The system SHALL parse stock order match notifications from broker apps (e.g. Stockbit) into structured trade data including stock ticker symbol, Yahoo Finance normalized instrument symbol, quantity in lots and shares, price per share, total amount, and trade action.

#### Scenario: Successfully parse Stockbit buy order match
- **WHEN** a notification arrives with text "Order Match: Pembelian 10 lot BBRI di harga Rp3.340 berhasil"
- **THEN** parser outputs `event_class = "expense"`, `amount = 3340000`, `symbol = "BBRI"`, `instrument_symbol = "BBRI.JK"`, `lots = 10`, `units = 1000`, `price = 3340`, and `action = "buy"`

#### Scenario: Successfully parse Stockbit sell order match
- **WHEN** a notification arrives with text "Order Match: Penjualan 5 lot BBRI di harga Rp3.400 berhasil"
- **THEN** parser outputs `event_class = "income"`, `amount = 1700000`, `symbol = "BBRI"`, `instrument_symbol = "BBRI.JK"`, `lots = 5`, `units = 500`, `price = 3400`, and `action = "sell"`

### Requirement: Stock Pocket Auto-Provisioning on Ingestion
When an investment stock buy notification is ingested for a broker account, the system SHALL automatically resolve or create a child account (pocket) under that broker account linked with the stock's instrument symbol, units, and average buy price.

#### Scenario: Auto-provisioning new stock pocket on first buy
- **WHEN** a buy notification for 10 lots of BBRI arrives and no pocket exists under the broker account
- **THEN** system creates a child account under the broker account with `name = "BBRI"`, `type = "investment"`, `instrument_type = "stock"`, `instrument_symbol = "BBRI.JK"`, `units = 1000`, and `avg_buy_price = 3340`

#### Scenario: Updating existing stock pocket position on subsequent buy
- **WHEN** a buy notification for 4 lots of BBRI at Rp3.340 arrives and an existing pocket has 1,000 units at Rp3.340
- **THEN** system updates the existing pocket to have `units = 1400` and recalculates `avg_buy_price = 3340`

### Requirement: Targeted Dual-Leg Transfer Routing
The system SHALL route investment trade transactions directly to the provisioned stock pocket leg, deducting or crediting the linked funding account without double-counting parent broker balances.

#### Scenario: Dual-leg stock purchase execution
- **WHEN** 10 lots of BBRI are purchased on Stockbit funded by RDN BCA
- **THEN** system records a `transfer` transaction with `account_id = RDN_BCA_ID` and `transfer_target_account_id = BBRI_POCKET_ID` for Rp 3.340.000, excluding it from daily budget consumption

### Requirement: Live Market Valuation and Automatic Price Sync
The system SHALL support live quote synchronization for provisioned stock pockets via Yahoo Finance and aggregate child market valuations and unrealized P&L into the parent broker account.

#### Scenario: Live price quote updates stock valuation
- **WHEN** price synchronization runs and Yahoo Finance returns Rp 3.320 for `BBRI.JK`
- **THEN** system updates `last_price` to 3320 on the `BBRI` pocket, calculating market value as `units * 3320`, capital gain as `(units * 3320) - (units * avg_buy_price)`, and rolls up the total balance to the master broker account

